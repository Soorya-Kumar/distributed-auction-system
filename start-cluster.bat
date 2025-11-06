@echo off
echo ============================================
echo   Distributed Auction System - Easy Start
echo ============================================
echo.

echo Cleaning up old processes...
for /f "tokens=5" %%a in ('netstat -ano ^| find "8001"') do taskkill /PID %%a /F >nul 2>&1
for /f "tokens=5" %%a in ('netstat -ano ^| find "8002"') do taskkill /PID %%a /F >nul 2>&1
for /f "tokens=5" %%a in ('netstat -ano ^| find "8003"') do taskkill /PID %%a /F >nul 2>&1

echo.
echo ============================================
echo   Starting Cluster Nodes
echo ============================================
echo.

:: Start three nodes (non-blocking)
start cmd /k "go run server/node.go node1 8001 localhost:8002 localhost:8003"
start cmd /k "go run server/node.go node2 8002 localhost:8001 localhost:8003"
start cmd /k "go run server/node.go node3 8003 localhost:8001 localhost:8002"

echo Waiting 4 seconds for nodes to initialize...
timeout /t 4 /nobreak >nul

echo.
echo ============================================
echo   Starting Web Interface
echo ============================================
echo.


start cmd /k "go run web-server.go"
echo Waiting for web server to start...
timeout /t 2 /nobreak >nul

start http://localhost:8080

echo.
echo ============================================
echo   System is Running!
echo ============================================
echo Press Ctrl+C in any window to stop nodes.
pause
