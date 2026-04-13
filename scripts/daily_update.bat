@echo off
chcp 65001 >nul 2>&1
REM ============================================================
REM daily_update.bat — OPT + GAJT daily auto update
REM Windows Task Scheduler runs this at 16:00 JST daily
REM ============================================================

set "REPO=C:\Users\1943b\Desktop\openrouter-price-data"
set "PYTHON=C:\Users\1943b\anaconda3\python.exe"
set "GIT=C:\Program Files\Git\bin\git.exe"
set "LOG=%REPO%\scripts\daily_update.log"

cd /d "%REPO%"

echo [%date% %time%] === START === >> "%LOG%" 2>&1

REM 1. Pull latest
"%GIT%" pull origin main >> "%LOG%" 2>&1

REM 2. OPT price update
echo [%date% %time%] Running diff_prices.py >> "%LOG%" 2>&1
"%PYTHON%" scripts/diff_prices.py >> "%LOG%" 2>&1
if %errorlevel% neq 0 (
    echo [%date% %time%] FAIL: diff_prices.py errorlevel=%errorlevel% >> "%LOG%" 2>&1
) else (
    echo [%date% %time%] OK: diff_prices.py >> "%LOG%" 2>&1
)

REM 3. GAJT update
echo [%date% %time%] Running gajt_update.py >> "%LOG%" 2>&1
"%PYTHON%" scripts/gajt_update.py >> "%LOG%" 2>&1
if %errorlevel% neq 0 (
    echo [%date% %time%] FAIL: gajt_update.py errorlevel=%errorlevel% >> "%LOG%" 2>&1
) else (
    echo [%date% %time%] OK: gajt_update.py >> "%LOG%" 2>&1
)

REM 4. Commit and push
"%GIT%" add data/ >> "%LOG%" 2>&1
"%GIT%" diff --staged --quiet >> "%LOG%" 2>&1
if %errorlevel% neq 0 (
    echo [%date% %time%] Committing changes >> "%LOG%" 2>&1
    "%GIT%" commit -m "daily update %date:~0,4%-%date:~5,2%-%date:~8,2%" >> "%LOG%" 2>&1
    "%GIT%" push origin main >> "%LOG%" 2>&1
    if %errorlevel% neq 0 (
        echo [%date% %time%] FAIL: git push errorlevel=%errorlevel% >> "%LOG%" 2>&1
        echo [%date% %time%] Retrying pull and push >> "%LOG%" 2>&1
        "%GIT%" pull --rebase origin main >> "%LOG%" 2>&1
        "%GIT%" push origin main >> "%LOG%" 2>&1
        if %errorlevel% neq 0 (
            echo [%date% %time%] FAIL: git push retry also failed >> "%LOG%" 2>&1
        ) else (
            echo [%date% %time%] OK: push succeeded on retry >> "%LOG%" 2>&1
        )
    ) else (
        echo [%date% %time%] OK: commit and push >> "%LOG%" 2>&1
    )
) else (
    echo [%date% %time%] No changes >> "%LOG%" 2>&1
)

echo [%date% %time%] === END === >> "%LOG%" 2>&1
