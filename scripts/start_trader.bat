@echo off
:: MT5 Trader — launcher with duplicate-instance guard
:: The Python code itself uses .trader.lock (msvcrt) as a second safeguard,
:: but this BAT gives a friendlier user-facing message.

cd /d "C:\pyProjects\MT_Connector"

:: Check if trader_main is already running
wmic process where "commandline like '%%src.trader_main%%' and name='python.exe'" get processid 2>nul | findstr /r "[0-9]" >nul
if %errorlevel%==0 (
    echo.
    echo   [!] MT5 Trader is already running.
    echo       Close the existing window first.
    echo.
    pause
    exit /b 1
)

echo   Starting MT5 Trader ...
"C:\pyProjects\MT_Connector\.venv\Scripts\python.exe" -m src.trader_main
if %errorlevel% neq 0 (
    echo.
    echo   [!] Trader exited with error code %errorlevel%
    pause
)
