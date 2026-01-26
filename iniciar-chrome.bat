@echo off
echo Iniciando Chrome con debugging remoto...
start chrome.exe --remote-debugging-port=9222
echo.
echo Chrome iniciado en modo debugging en puerto 9222
echo Ahora puedes ejecutar: node spotifyScraper.js
pause
