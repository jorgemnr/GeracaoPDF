REM Windows batch script to run 1+ Python program/scripts, sequentially, within
REM their virtual environment. This can be called from Windows Task Scheduler.

@REM PARAMETROS
set arquivo_python=gerar_PDF.py

@REM PARAMETROS INTERNOS
set pasta_projeto=%~dp0
set pasta_venv=%pasta_projeto%.venv\Scripts

@REM ATIVAR AMBIENTE VIRTUAL
call %pasta_venv%\activate.bat

@REM EXECUTAR PROJETO
call python.exe %pasta_projeto%%arquivo_python%

@REM DESATIVAR AMBIENTE
call %pasta_venv%\deactivate.bat
exit /B 1