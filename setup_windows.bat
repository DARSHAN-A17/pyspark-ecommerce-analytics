@echo off
REM Setup helper for the BDA E-Commerce Analytics project (Windows).
REM Creates a project-local virtual environment, installs dependencies,
REM then runs the environment check. Run from the project folder.
setlocal

where python >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Python not found on PATH. Install Python 3.10+ from python.org and re-run.
    exit /b 1
)

if not exist ".venv" (
    echo Creating project virtual environment in .venv ...
    python -m venv .venv
    if errorlevel 1 (
        echo [ERROR] Could not create the virtual environment.
        exit /b 1
    )
) else (
    echo Virtual environment .venv already exists, reusing it.
)

call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
if errorlevel 1 (
    echo [ERROR] Dependency installation failed. Check your internet connection and re-run.
    exit /b 1
)

python check_environment.py
if errorlevel 1 (
    echo [ERROR] Environment check failed. Fix the messages above, then re-run.
    exit /b 1
)

echo.
echo Setup complete. Next steps:
echo   1. Copy Online Retail.xlsx into this folder if it is not here already.
echo   2. Launch the project with:  jupyter notebook BDA_Complete_Analytics.ipynb
endlocal
