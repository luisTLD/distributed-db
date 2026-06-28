@echo off
REM Sobe um cluster local de 3 nos no Windows, cada um na propria janela.
cd /d "%~dp0.."
if not exist data mkdir data

echo Starting node 1 (port 50051)...
start "distdb node 1" cmd /k python -m distdb.node --id 1 --data-dir data
timeout /t 1 >nul

echo Starting node 2 (port 50052)...
start "distdb node 2" cmd /k python -m distdb.node --id 2 --data-dir data
timeout /t 1 >nul

echo Starting node 3 (port 50053, initial leader)...
start "distdb node 3" cmd /k python -m distdb.node --id 3 --data-dir data

echo.
echo Cluster starting in three new windows.
echo Open a client in THIS window with:  python -m distdb.client
