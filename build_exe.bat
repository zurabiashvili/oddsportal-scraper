@echo off
REM Build standalone .exe with PyInstaller, then copy to Desktop

pip install pyinstaller
pyinstaller --onefile --windowed --name "OddsPortal_Scraper" --distpath "%USERPROFILE%\Desktop" app.py

echo.
echo If successful, OddsPortal_Scraper.exe is on your Desktop.
echo Note: First run may need "playwright install chromium" if browser is missing.
pause
