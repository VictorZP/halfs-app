@echo off
echo Building Excel Analyzer Pro...
pyinstaller --distpath . "Excel Analyzer Pro.spec"
echo.
echo Done! Exe is in the current directory.
pause
