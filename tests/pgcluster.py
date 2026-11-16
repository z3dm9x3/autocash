"""A PostgreSQL cluster for tests (initdb once, pg_ctl on a free port).

Tests need a REAL PostgreSQL that can be stopped and started (AGENTS.md, rule 6). The cluster
lives in its own directory and never touches any other PostgreSQL installed on the machine.

initdb is slow on a busy disk (minutes), so the data directory is created once and reused by
later runs: $AUTOCASH_PG_TESTDIR, by default <temp>/autocash_pg_test. Test databases are
dropped after each test; leftovers of a killed run are dropped at the next start.
Binaries: $AUTOCASH_PG_BIN, then PATH, then the usual install locations.
"""

from __future__ import annotations

import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

TEST_DB = re.compile(r"^t\d+$")


def find_pg_bin() -> Path:
    exe = ".exe" if sys.platform == "win32" else ""
    candidates: list[Path] = []
    if os.environ.get("AUTOCASH_PG_BIN"):
        candidates.append(Path(os.environ["AUTOCASH_PG_BIN"]))
    which = shutil.which("initdb")
    if which:
        candidates.append(Path(which).parent)
    roots = [Path(r"C:\Program Files\PostgreSQL"), Path("/usr/lib/postgresql"), Path("/usr/pgsql")]
    for root in roots:
        if root.is_dir():
            candidates.extend(sorted((p / "bin" for p in root.iterdir()), reverse=True))
    for cand in candidates:
        if (cand / f"initdb{exe}").exists() and (cand / f"pg_ctl{exe}").exists():
            return cand
    raise RuntimeError("PostgreSQL binaries not found; set AUTOCASH_PG_BIN to the directory with initdb/pg_ctl")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class PgCluster:
    def __init__(self, directory: Path | None = None) -> None:
        self.bin = find_pg_bin()
        default = Path(tempfile.gettempdir()) / "autocash_pg_test"
        self.dir = directory or Path(os.environ.get("AUTOCASH_PG_TESTDIR", default))
        self.port = _free_port()
        self.running = False

    @property
    def data(self) -> Path:
        return self.dir / "data"

    def _run(self, *args: str, timeout: float = 600) -> None:
        subprocess.run(
            [str(self.bin / args[0]), *args[1:]], check=True, timeout=timeout,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    def _status(self) -> int:
        return subprocess.run([str(self.bin / "pg_ctl"), "-D", str(self.data), "status"],
                              stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                              timeout=60).returncode

    def init(self) -> None:
        if (self.data / "PG_VERSION").exists():
            if self._status() == 0:
                # A previous run was killed with the server still up: stop it, it may be on another port.
                self._run("pg_ctl", "-D", str(self.data), "-m", "immediate", "-w", "-t", "240", "stop")
            return
        shutil.rmtree(self.data, ignore_errors=True)  # a half-made directory of an interrupted initdb
        self.dir.mkdir(parents=True, exist_ok=True)
        tmp = self.dir / "data.init"
        shutil.rmtree(tmp, ignore_errors=True)
        self._run("initdb", "-D", str(tmp), "-U", "postgres", "-A", "trust", "-E", "UTF8", "--locale=C")
        tmp.rename(self.data)  # only a complete cluster gets the final name

    def start(self) -> None:
        opts = f"-p {self.port} -c listen_addresses=127.0.0.1 -c max_connections=60 -c fsync=off"
        self._run("pg_ctl", "-D", str(self.data), "-l", str(self.dir / "pg.log"), "-w", "-t", "300",
                  "-o", opts, "start")
        self.running = True

    def stop(self, mode: str = "fast") -> None:
        if self.running:
            # The shutdown checkpoint can take over a minute on a slow disk; pg_ctl gives up after 60 s.
            self._run("pg_ctl", "-D", str(self.data), "-m", mode, "-w", "-t", "240", "stop")
            self.running = False

    def destroy(self) -> None:
        """Stop the server; the data directory stays for the next run."""
        self.stop("fast")

    def dsn(self, database: str = "postgres") -> str:
        return f"postgresql://postgres@127.0.0.1:{self.port}/{database}"
