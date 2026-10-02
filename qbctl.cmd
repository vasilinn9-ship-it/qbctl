@echo off
setlocal
set PYTHONUTF8=1
set PYTHONDONTWRITEBYTECODE=1
py -3 "%~dp0qbctl_entry.py" %*
exit /b %errorlevel%
