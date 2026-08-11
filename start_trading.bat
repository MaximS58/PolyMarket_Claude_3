@echo off
REM ===========================================================================
REM  Polymarket Paper Trading - launcher
REM
REM  Double-click this file, or run it from any terminal. Does not need VS Code.
REM
REM    1. Refreshes the pipeline data (about 4-5 minutes) so we don't trade a
REM       slate of markets that already resolved.
REM    2. Opens Windows Terminal with two panes:
REM         left  - paper_trade.py         (live portfolio dashboard, 30s loop)
REM         right - trader_exit_monitor.py (follow-the-traders exit signals)
REM
REM  Ctrl+C in a pane stops that process. Panes stay open so you can read the
REM  final state. Full logs are always in PolyMarket_Code\output\*.log
REM ===========================================================================

setlocal
cd /d "%~dp0PolyMarket_Code"

echo.
echo  Refreshing trading data before we start...
echo.

python refresh_data.py
if errorlevel 1 goto :refresh_failed

where wt.exe >nul 2>nul
if errorlevel 1 goto :no_wt

echo.
echo  Opening the two-pane trading view...
echo.

REM  -V splits vertically, i.e. the new pane sits to the RIGHT (side by side).
start "" wt.exe -d "%CD%" cmd /k python paper_trade.py --reset ; split-pane -V -d "%CD%" cmd /k python trader_exit_monitor.py
goto :eof

:no_wt
echo.
echo  Windows Terminal (wt.exe) not found - falling back to a single window.
echo.
python run_paper_trading.py --reset
goto :eof

:refresh_failed
echo.
echo  ============================================================
echo   Data refresh FAILED - not starting the trader.
echo   Check the output above and PolyMarket_Code\output\*.log
echo  ============================================================
echo.
pause
exit /b 1
