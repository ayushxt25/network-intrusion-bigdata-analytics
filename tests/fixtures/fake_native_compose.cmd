@echo off
if /I "%~1"=="fail" (
  echo ERROR: simulated native failure 1>&2
  exit /b 7
)
echo WARNING: simulated harmless native stderr warning 1>&2
echo simulated stdout
exit /b 0
