@echo off
cd /d "%~dp0"
pyinstaller --noconfirm --onefile --windowed ^
  --icon=siamp_icon.ico ^
  --add-data "mydata/ref_files.cfg;mydata" ^
  --add-data "mydata/siamp_icon.ico;mydata" ^
  --version-file meta_info.res ^
  ETL_SIAMP_GUI.py
pause
