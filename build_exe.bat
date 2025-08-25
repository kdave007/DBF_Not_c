@echo off
echo Building DBF_Recibos executable...

rem Clean previous build artifacts
rmdir /s /q build dist
del /q *.spec

rem Run PyInstaller with all required options
pyinstaller --noconfirm ^
  --onefile ^
  --name "DBF_Recibos" ^
  --add-data ".env;." ^
  --add-data "mappings_dv.json;." ^
  --add-data "tests\art_m.json;tests" ^
  --add-data "Advantage.Data.Provider.dll;." ^
  --hidden-import=src ^
  --hidden-import=src.config ^
  --hidden-import=src.controllers ^
  --hidden-import=src.db ^
  --hidden-import=src.utils ^
  --paths="." ^
  tests\test_find_matches_simple.py

echo.
if %ERRORLEVEL% EQU 0 (
  echo Build completed successfully! Executable is in the 'dist' folder.
) else (
  echo Build failed with error code %ERRORLEVEL%
)
pause
