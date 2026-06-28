@echo off
REM Roda tudo (testes, cluster, demo, benchmark, estresse) e junta os logs em logs\coleta\.
setlocal

REM ---- vai para a raiz do projeto (pasta que contem este scripts\) ----
cd /d "%~dp0.."

REM ---- ativa o venv automaticamente, se existir ----
if exist "venv\Scripts\activate.bat" call "venv\Scripts\activate.bat"

REM ---- parametros (pode ajustar) ----
set OPS=200
set CLIENTS=8
set CLIENT_OPS=100

REM ---- zera a base para uma coleta limpa e reproduzivel ----
if exist data rmdir /s /q data
if exist logs rmdir /s /q logs
mkdir data
mkdir logs
mkdir logs\coleta

echo ==============================================================
echo  1/7  Testes automatizados (sem rede)
echo ==============================================================
python tests\run_all.py > logs\coleta\01_tests.txt 2>&1
type logs\coleta\01_tests.txt

echo.
echo ==============================================================
echo  2/7  Subindo o cluster de 3 nos (background)
echo ==============================================================
start "distdb-node-1" /min cmd /c "python -m distdb.node --id 1 --data-dir data > logs\node1.log 2>&1"
timeout /t 1 >nul
start "distdb-node-2" /min cmd /c "python -m distdb.node --id 2 --data-dir data > logs\node2.log 2>&1"
timeout /t 1 >nul
start "distdb-node-3" /min cmd /c "python -m distdb.node --id 3 --data-dir data > logs\node3.log 2>&1"
echo   aguardando a eleicao inicial (~5s)...
timeout /t 5 >nul

echo.
echo ==============================================================
echo  3/7  Demo do cliente
echo ==============================================================
python -m distdb.client --demo > logs\coleta\02_client_demo.txt 2>&1
type logs\coleta\02_client_demo.txt

echo.
echo ==============================================================
echo  4/7  Benchmark (distribuido x maquina unica), ops=%OPS%
echo ==============================================================
python scripts\benchmark.py --ops %OPS% > logs\coleta\03_benchmark.txt 2>&1
type logs\coleta\03_benchmark.txt

echo.
echo ==============================================================
echo  5/7  Estresse: %CLIENTS% clientes x %CLIENT_OPS% ops + consistencia
echo ==============================================================
python scripts\stress_test.py --clients %CLIENTS% --ops %CLIENT_OPS% > logs\coleta\04_stress.txt 2>&1
type logs\coleta\04_stress.txt

echo.
echo ==============================================================
echo  6/7  Copiando os logs dos nos
echo ==============================================================
copy /y logs\node1.log logs\coleta\05_node1.log >nul
copy /y logs\node2.log logs\coleta\05_node2.log >nul
copy /y logs\node3.log logs\coleta\05_node3.log >nul

echo.
echo ==============================================================
echo  7/7  Consolidando em TUDO.txt e encerrando o cluster
echo ==============================================================
REM ---- junta todos os arquivos num so ----
set OUT=logs\coleta\TUDO.txt
if exist "%OUT%" del "%OUT%"
for %%F in (01_tests.txt 02_client_demo.txt 03_benchmark.txt 04_stress.txt 05_node1.log 05_node2.log 05_node3.log) do (
  echo.>> "%OUT%"
  echo ##########################################################>> "%OUT%"
  echo ##### %%F>> "%OUT%"
  echo ##########################################################>> "%OUT%"
  type "logs\coleta\%%F">> "%OUT%"
)

REM ---- encerra os nos ----
taskkill /F /FI "WINDOWTITLE eq distdb-node-1*" >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq distdb-node-2*" >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq distdb-node-3*" >nul 2>&1

echo.
echo ==============================================================
echo  PRONTO!  Me mande este arquivo:
echo     %CD%\logs\coleta\TUDO.txt
echo ==============================================================
echo (se preferir, mande a pasta inteira: logs\coleta\)
pause
endlocal
