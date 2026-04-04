@echo off
setlocal
cd /d "%~dp0"

where g++ >nul 2>&1
if errorlevel 1 (
  echo Add MinGW-w64 g++.exe to PATH, or install WinLibs / MSYS2 MinGW, then run this script again.
  exit /b 1
)

echo Building tracker.exe ...
g++ -std=c++11 -O2 -pthread tracker.cpp -o tracker.exe -lws2_32
if errorlevel 1 exit /b 1

echo Building client.exe ...
g++ -std=c++11 -O2 -pthread client.cpp -o client.exe -lws2_32
if errorlevel 1 exit /b 1

echo Done: tracker.exe and client.exe
exit /b 0
