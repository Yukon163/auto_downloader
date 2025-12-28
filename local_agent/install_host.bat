@echo off
REM Install Native Messaging Host for Chrome
REM Run this script as Administrator

set MANIFEST_PATH=%~dp0com.autodownloader.agent.json
set REG_KEY=HKCU\SOFTWARE\Google\Chrome\NativeMessagingHosts\com.autodownloader.agent

echo Installing Native Messaging Host...
echo Manifest path: %MANIFEST_PATH%

REM Add registry key
reg add "%REG_KEY%" /ve /t REG_SZ /d "%MANIFEST_PATH%" /f

if %ERRORLEVEL% EQU 0 (
    echo.
    echo SUCCESS! Native Messaging Host registered.
    echo.
    echo IMPORTANT: You need to update the manifest file with your extension ID:
    echo 1. Load the Chrome extension from chrome-ext folder
    echo 2. Copy the extension ID from chrome://extensions
    echo 3. Edit com.autodownloader.agent.json
    echo 4. Replace EXTENSION_ID_PLACEHOLDER with your actual extension ID
) else (
    echo.
    echo FAILED to register Native Messaging Host.
)

pause
