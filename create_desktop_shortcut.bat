@echo off
REM Creates OddsPortal Scraper shortcut on Desktop

set PROJECT_DIR=%~dp0
set DESKTOP=%USERPROFILE%\Desktop

REM Create a batch file launcher on Desktop
(
echo @echo off
echo cd /d "%PROJECT_DIR%"
echo python app.py
echo pause
) > "%DESKTOP%\OddsPortal Scraper.bat"

echo Created: %DESKTOP%\OddsPortal Scraper.bat
echo Double-click it to run the scraper.
pause
