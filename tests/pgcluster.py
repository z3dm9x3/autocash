"""A throw-away PostgreSQL cluster for tests (initdb + pg_ctl on a free port).

Tests need a REAL PostgreSQL that can be stopped and started (AGENTS.md, rule 6). The cluster
lives in a temporary directory and never touches any other PostgreSQL installed on the machine.
Binaries: $AUTOCASH_PG_BIN, then PATH, then the usual install locations.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path


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
    def __init__(self) -> None:
        self.bin = find_pg_bin()
        self.dir = Path(tempfile.mkdtemp(prefix="autocash_pg_"))
        self.port = _free_port()
        self.running = False

    def _run(self, *args: str, timeout: float = 300) -> None:
        subprocess.run(
            [str(self.bin / args[0]), *args[1:]], check=True, timeout=timeout,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    def init(self) -> None:
        self._run("initdb", "-D", str(self.dir / "data"), "-U", "postgres", "-A", "trust", "-E", "UTF8",
                  "--locale=C")

    def start(self) -> None:
        opts = f"-p {self.port} -c listen_addresses=127.0.0.1 -c max_connections=60"
        self._run("pg_ctl", "-D", str(self.dir / "data"), "-l", str(self.dir / "pg.log"), "-w", "-t", "240",
                  "-o", opts, "start")
        self.running = True

    def stop(self, mode: str = "fast") -> None:
        if self.running:
            # The shutdown checkpoint of a cluster with many test databases can take over a minute
            # on a slow disk; pg_ctl gives up after 60 s by default.
            self._run("pg_ctl", "-D", str(self.dir / "data"), "-m", mode, "-w", "-t", "240", "stop")
            self.running = False

    def destroy(self) -> None:
        try:
            self.stop("immediate")
        finally:
            shutil.rmtree(self.dir, ignore_errors=True)

    def dsn(self, database: str = "postgres") -> str:
        return f"postgresql://postgres@127.0.0.1:{self.port}/{database}"
