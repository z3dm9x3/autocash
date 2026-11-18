# Autocash on a Windows dev machine: start / stop / status / log (docs/ops.md).
#
#   .\deploy\windows\autocash-run.ps1 start|stop|status|log [-Lines 50]
#
# Reads AUTOCASH_PG_DSN from "Documents\AutoCash Secrets\pg.env" and the session key from
# "Documents\AutoCash Secrets\session.key"; neither is printed. The service runs detached (a hidden
# window); its log goes to "Documents\AutoCash Data\logs\autocash.log". One process only.
param(
    [Parameter(Mandatory = $true, Position = 0)][ValidateSet("start", "stop", "status", "log")][string]$Action,
    [int]$Lines = 50,
    [string]$Secrets = (Join-Path $env:USERPROFILE "Documents\AutoCash Secrets"),
    [string]$DataRoot = (Join-Path $env:USERPROFILE "Documents\AutoCash Data")
)
$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$python = Join-Path $root ".venv\Scripts\python.exe"
$logDir = Join-Path $DataRoot "logs"
$logFile = Join-Path $logDir "autocash.log"
$pidFile = Join-Path $DataRoot "autocash.pid"

function Get-Running {
    if (-not (Test-Path $pidFile)) { return $null }
    $procId = [int](Get-Content $pidFile -Raw)
    $p = Get-Process -Id $procId -ErrorAction SilentlyContinue
    if ($p -and $p.ProcessName -like "python*") { return $p }
    return $null
}

switch ($Action) {
    "start" {
        if (Get-Running) { Write-Output "already running (PID $((Get-Running).Id))"; break }
        $free = (Get-PSDrive ($DataRoot.Substring(0, 1))).Free / 1MB
        if ($free -lt 500) { throw "only $([int]$free) MB free on the disk: free some space first" }
        New-Item -ItemType Directory -Force $logDir | Out-Null
        $dsn = (Get-Content (Join-Path $Secrets "pg.env") | Where-Object { $_ -like "AUTOCASH_PG_DSN=*" }) -replace "^AUTOCASH_PG_DSN=", ""
        if (-not $dsn) { throw "AUTOCASH_PG_DSN not found in $Secrets\pg.env" }
        $env:AUTOCASH_PG_DSN = $dsn
        $env:AUTOCASH_SESSION_KEY_FILE = Join-Path $Secrets "session.key"
        $env:PYTHONIOENCODING = "utf-8"
        $p = Start-Process -FilePath $python -WorkingDirectory $root -WindowStyle Hidden -PassThru `
            -ArgumentList @("-m", "autocash.cli.run", "--config", "config", "--data", "data") `
            -RedirectStandardError $logFile -RedirectStandardOutput (Join-Path $logDir "autocash.out.log")
        Remove-Item Env:AUTOCASH_PG_DSN
        Set-Content -Path $pidFile -Value $p.Id
        Write-Output "started (PID $($p.Id)); log: $logFile"
    }
    "stop" {
        $p = Get-Running
        if (-not $p) { Write-Output "not running"; break }
        # A hard stop is safe: everything accepted is in SQLite (WAL) and is processed after the start.
        Stop-Process -Id $p.Id -Force
        Remove-Item $pidFile -ErrorAction SilentlyContinue
        Write-Output "stopped (PID $($p.Id))"
    }
    "status" {
        $p = Get-Running
        if ($p) { Write-Output "running (PID $($p.Id), since $($p.StartTime))" } else { Write-Output "not running" }
    }
    "log" { if (Test-Path $logFile) { Get-Content $logFile -Tail $Lines } else { Write-Output "no log yet" } }
}
