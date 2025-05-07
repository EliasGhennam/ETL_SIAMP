!define APP_NAME "ETL SIAMP"
!define VERSION "1.0.0"
!define EXE_NAME "ETL SIAMP.exe"

Outfile "ETL_SIAMP_Installer.exe"
InstallDir "$PROGRAMFILES\${APP_NAME}"

RequestExecutionLevel admin
SetCompress auto
SetCompressor lzma

Icon "siamp_icon.ico"
XPStyle on

Page directory
Page instfiles

Section "Install"

  SetOutPath "$INSTDIR"
  File "..\dist\${EXE_NAME}"

  ; Créer un raccourci sur le bureau
  CreateShortcut "$DESKTOP\${APP_NAME}.lnk" "$INSTDIR\${EXE_NAME}" "" "$INSTDIR\${EXE_NAME}" 0

  ; Créer un raccourci dans le menu démarrer
  CreateDirectory "$SMPROGRAMS\${APP_NAME}"
  CreateShortcut "$SMPROGRAMS\${APP_NAME}\${APP_NAME}.lnk" "$INSTDIR\${EXE_NAME}" "" "$INSTDIR\${EXE_NAME}" 0

SectionEnd

Section "Uninstall"
  Delete "$INSTDIR\${EXE_NAME}"
  Delete "$DESKTOP\${APP_NAME}.lnk"
  Delete "$SMPROGRAMS\${APP_NAME}\${APP_NAME}.lnk"
  RMDir "$SMPROGRAMS\${APP_NAME}"
  RMDir "$INSTDIR"
SectionEnd
