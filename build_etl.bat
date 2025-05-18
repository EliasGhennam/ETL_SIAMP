@echo off
cd /d "%~dp0"
nuitka ^
  --standalone ^
  --onefile ^
  --enable-plugin=pyqt6 ^
  --include-data-files=siamp_icon.ico=mydata/ ^
  --include-data-files=ref_files.cfg=mydata/ ^
  --output-dir=build_etl ^
  --windows-icon-from-ico=siamp_icon.ico ^
  --lto=yes ^
  --no-pyi-file ^
  --nofollow-import-to=tkinter ^
  ETL_SIAMP_GUI.py
pause
