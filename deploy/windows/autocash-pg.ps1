# The separate PostgreSQL server of Autocash on a Windows dev machine (D-RM-9).
# It is NOT the PostgreSQL of other projects: own data directory, own port (5434), own password.
#
#   .\deploy\windows\autocash-pg.ps1 start|stop|status [-DataDir <path>]
#
# The server is started detached (a hidden window): a server started from a console that closes
# dies with it (Windows error 0xC0000142). Secrets: "Documents\AutoCash Secrets\pg.env", never in git.
param(
    [Parameter(Mandatory = $true, Position = 0)][ValidateSet("start", "stop", "status")][string]$Action,
    [string]$DataDir = (Join-Path $env:USERPROFILE "Documents\AutoCash Data\pg"),
    [string]$PgBin = "C:\Program Files\PostgreSQL\17\bin",
    [int]$Port = 5434
)
$ErrorActionPreference = "Stop"
$pgctl = Join-Path $PgBin "pg_ctl.exe"
$data = Join-Path $DataDir "data"
$logFile = Join-Path $DataDir "pg.log"

function Test-Listening { (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Measure-Object).Count -gt 0 }

switch ($Action) {
    "start" {
        if (Test-Listening) { Write-Output "already running on port $Port"; break }
        $free = (Get-PSDrive ($DataDir.Substring(0, 1))).Free / 1MB
        if ($free -lt 500) { throw "only $([int]$free) MB free on the disk of ${DataDir}: free some space first" }
        Start-Process -FilePath $pgctl -ArgumentList @("-D", "`"$data`"", "-l", "`"$logFile`"", "start") -WindowStyle Hidden
        for ($i = 0; $i -lt 90; $i++) {
            if ((Test-Listening) -and (Select-String -Path $logFile -Pattern "ready to accept" -Quiet)) { break }
            Start-Sleep -Seconds 2
        }
        if (Test-Listening) { Write-Output "running on 127.0.0.1:$Port" } else { throw "not started: see $logFile" }
    }
    "stop" { & $pgctl -D $data -m fast -w -t 120 stop }
    "status" {
        & $pgctl -D $data status
        if (Test-Listening) { Write-Output "listening on 127.0.0.1:$Port" }
    }
}
