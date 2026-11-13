@echo off
setlocal
set "HERE=%~dp0"
for %%I in ("%HERE%..") do set "ROOT=%%~fI\"
if not exist "%HERE%add_account.py" (
    echo ERROR: add_account.py not found next to this file.
    pause
    exit /b 1
)
if not exist "%ROOT%.venv\Scripts\python.exe" (
    echo Creating virtual environment...
    py -3 -m venv "%ROOT%.venv" 2>nul || python -m venv "%ROOT%.venv"
    if errorlevel 1 (
        echo ERROR: could not create .venv. Install Python 3.10+ from python.org
        pause
        exit /b 1
    )
    echo Installing dependencies...
    "%ROOT%.venv\Scripts\python.exe" -m pip install -r "%ROOT%requirements.txt"
    if errorlevel 1 (
        echo ERROR: dependency install failed
        pause
        exit /b 1
    )
)
"%ROOT%.venv\Scripts\python.exe" "%HERE%add_account.py"
echo.
pause
