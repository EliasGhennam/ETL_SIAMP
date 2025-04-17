#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ETL_SIAMP_GUI.py – Interface PyQt6 améliorée
-------------------------------------------------
• Mode API / Manuel avec masquage dynamique.
• Sélecteur de date (calendrier popup) pour taux historiques.
• Glisser‑déposer de fichiers Excel + ajout/retrait.
• Console en temps réel + barre de progression.
• Exécute le script core `ETL_SIAMP.py` via subprocess.
• Charge et sauvegarde la clé API.
"""
from __future__ import annotations
import os
import sys
import subprocess
from typing import List

from PyQt6.QtCore   import Qt, QThread, pyqtSignal, QDate
from PyQt6.QtGui    import QIcon, QAction, QKeySequence
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QPushButton, QFileDialog, QMessageBox, QListWidget, QComboBox,
    QPlainTextEdit, QProgressBar, QDateEdit
)

SCRIPT_CORE   = "ETL_SIAMP.py"
ICON_PATH     = "siamp_icon.ico"
CONFIG_FILE   = "siamp_api_key.cfg"
***REMOVED***


# ---------------------------------------------------------------- worker QThread
class Worker(QThread):
    log      = pyqtSignal(str)
    progress = pyqtSignal(int)
    done     = pyqtSignal(bool)

    def __init__(self, cmd: list[str], env: dict[str,str]):
        super().__init__()
        self.cmd = cmd
        self.env = env

    def run(self):
        proc = subprocess.Popen(
            self.cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",    # force UTF‑8 pour éviter UnicodeDecodeError
            errors="replace",
            env=self.env
        )
        for line in proc.stdout:
            line = line.rstrip()
            self.log.emit(line)
            if line.startswith("PROGRESS:"):
                try:
                    pct = int(line.split(":")[1].strip().strip("% "))
                    self.progress.emit(pct)
                except ValueError:
                    pass
        self.done.emit(proc.wait() == 0)


# ---------------------------------------------------------------- DropListWidget
class DropListWidget(QListWidget):
    """Zone de liste acceptant le glisser‑déposer de fichiers .xlsx"""
    def __init__(self):
        super().__init__()
        self.setAcceptDrops(True)
        self.setSelectionMode(self.SelectionMode.ExtendedSelection)
        self.setMinimumHeight(150)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dragMoveEvent(self, event):
        event.acceptProposedAction()

    def dropEvent(self, event):
        for url in event.mimeData().urls():
            f = url.toLocalFile()
            if f.lower().endswith(".xlsx") and f not in self.files():
                self.addItem(f)
        event.acceptProposedAction()

    def files(self) -> List[str]:
        return [self.item(i).text() for i in range(self.count())]


# ---------------------------------------------------------------- MainWindow
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ETL SIAMP — Fusion Excel")
        self.setWindowIcon(QIcon(ICON_PATH))
        self.resize(760, 640)
        self._build_ui()
        self._apply_style()

    # ---------- UI ----------
    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(12)

        # ► Mode API / Manuel
        row_mode = QHBoxLayout()
        row_mode.addWidget(QLabel("Mode de conversion :"))
        self.cbo_mode = QComboBox()
        self.cbo_mode.addItems(["API", "Manuel"])
        self.cbo_mode.currentTextChanged.connect(self._toggle_mode)
        row_mode.addWidget(self.cbo_mode)
        row_mode.addStretch()
        layout.addLayout(row_mode)

        # ► Clé API
        self.row_api = QHBoxLayout()
        self.row_api.addWidget(QLabel("Clé API (currencyapi.net) :"))
        self.txt_api = QLineEdit(self._load_api())
        self.txt_api.setPlaceholderText("clé API")
        self.row_api.addWidget(self.txt_api)
        layout.addLayout(self.row_api)

        # ► Taux manuel
        self.row_manual = QHBoxLayout()
        self.row_manual.addWidget(QLabel("Taux manuels (USD=0.93,GBP=1.15) :"))
        self.txt_manual = QLineEdit()
        self.row_manual.addWidget(self.txt_manual)
        layout.addLayout(self.row_manual)

        # ► Sélecteur de date
        row_date = QHBoxLayout()
        row_date.addWidget(QLabel("Date des taux :"))
        self.date_edit = QDateEdit(QDate.currentDate())
        self.date_edit.setCalendarPopup(True)
        row_date.addWidget(self.date_edit)
        layout.addLayout(row_date)

        # ► Liste de fichiers
        layout.addWidget(QLabel("Fichiers Excel :"))
        self.lst_files = DropListWidget()
        layout.addWidget(self.lst_files)

        # ► Boutons Ajouter / Retirer
        btn_bar = QHBoxLayout()
        btn_add = QPushButton("Ajouter…")
        btn_add.clicked.connect(self._add_files)
        btn_bar.addWidget(btn_add)
        btn_rem = QPushButton("Retirer sélection")
        btn_rem.clicked.connect(self._remove_files)
        btn_bar.addWidget(btn_rem)
        btn_bar.addStretch()
        layout.addLayout(btn_bar)
        self.lst_files.setAlternatingRowColors(True)

        # ► Touche Suppr
        delete_act = QAction(
            self,
            shortcut=QKeySequence(Qt.Key.Key_Delete),
            triggered=self._remove_files
        )
        self.lst_files.addAction(delete_act)

        # ► Chemin de sortie
        row_out = QHBoxLayout()
        row_out.addWidget(QLabel("Fichier de sortie :"))
        self.txt_out = QLineEdit("fusion.xlsx")
        btn_out = QPushButton("Parcourir…")
        btn_out.clicked.connect(self._choose_output)
        row_out.addWidget(self.txt_out)
        row_out.addWidget(btn_out)
        layout.addLayout(row_out)

        # ► Barre de progression
        self.pbar = QProgressBar()
        self.pbar.setMaximum(100)
        self.pbar.setValue(0)
        layout.addWidget(self.pbar)

        # ► Bouton Lancer
        btn_run = QPushButton("▶ Lancer")
        btn_run.setMinimumHeight(38)
        btn_run.clicked.connect(self._run_etl)
        layout.addWidget(btn_run)

        # ► Console intégrée
        self.txt_log = QPlainTextEdit()
        self.txt_log.setReadOnly(True)
        self.txt_log.setMaximumBlockCount(1000)
        layout.addWidget(self.txt_log, stretch=2)

        # initialise la visibilité
        self._toggle_mode(self.cbo_mode.currentText())

    # ---------- style ----------
    def _apply_style(self):
        self.setStyleSheet("""
            QWidget { font-family: 'Segoe UI', sans-serif; font-size: 10pt; color: #E0E0E0; }
            QMainWindow { background-color: #22252A; }
            QLabel { font-weight: 500; }
            QLineEdit, QListWidget, QComboBox, QPlainTextEdit { 
                background-color: #2D3036; border: 1px solid #444; padding: 4px; border-radius: 4px; 
            }
            QPushButton { background-color: #44576D; border: none; padding: 8px 12px; border-radius: 4px; }
            QPushButton:hover { background-color: #527191; }
            QPushButton:pressed { background-color: #3C4E65; }
            QListWidget { border: 1px dashed #555; }
        """)

    # ---------- slots ----------
    def _toggle_mode(self, mode: str):
        api = (mode == "API")
        for w in self._iter_widgets(self.row_api):
            w.setVisible(api)
        for w in self._iter_widgets(self.row_manual):
            w.setVisible(not api)

    @staticmethod
    def _iter_widgets(layout):
        return (layout.itemAt(i).widget() for i in range(layout.count()))

    def _add_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Sélectionner fichiers", "", "Excel (*.xlsx)")
        for f in files:
            if f not in self.lst_files.files():
                self.lst_files.addItem(f)

    def _remove_files(self):
        for item in self.lst_files.selectedItems():
            self.lst_files.takeItem(self.lst_files.row(item))

    def _choose_output(self):
        path, _ = QFileDialog.getSaveFileName(self, "Fichier de sortie", self.txt_out.text(), "Excel (*.xlsx)")
        if path:
            self.txt_out.setText(path)

    def _run_etl(self):
        files = self.lst_files.files()
        if not files:
            return QMessageBox.warning(self, "Erreur", "Ajoutez au moins un fichier Excel.")
        out = self.txt_out.text().strip()
        if not out:
            return QMessageBox.warning(self, "Erreur", "Spécifiez le fichier de sortie.")

        mode = self.cbo_mode.currentText()
        api  = self.txt_api.text().strip()
        man  = self.txt_manual.text().strip()

        if mode == "API" and not api:
            return QMessageBox.warning(self, "Erreur", "Saisissez la clé API.")
        if mode == "Manuel" and not man:
            return QMessageBox.warning(self, "Erreur", "Saisissez les taux manuels ou changez de mode.")

        if mode == "API":
            self._save_api_key(api)

        # récupère la date au format YYYY‑MM‑DD
        date_str = self.date_edit.date().toString("yyyy-MM-dd")

        cmd = [
            sys.executable, SCRIPT_CORE,
            "--date", date_str,
            "--chemin_sortie", out,
            "--fichiers", *files
        ]
        if man:
            cmd += ["--taux_manuels", man]

        env = dict(os.environ, GOOEY="0")

        # reset UI
        self.txt_log.clear()
        self.pbar.setValue(0)

        # démarre le worker
        self.worker = Worker(cmd, env)
        self.worker.log.connect(self.txt_log.appendPlainText)
        self.worker.progress.connect(self.pbar.setValue)
        self.worker.done.connect(self._on_done)
        self.worker.start()

    def _on_done(self, ok: bool):
        self.pbar.setValue(100 if ok else 0)
        QMessageBox.information(
            self,
            "Terminé" if ok else "Erreur",
            "Traitement terminé avec succès !" if ok else "Le script a échoué."
        )

    # ---------- helpers ----------
    def _load_api(self) -> str:
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return f.read().strip() or DEFAULT_API
        except FileNotFoundError:
            return DEFAULT_API

    def _save_api_key(self, key: str):
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                f.write(key.strip())
        except Exception:
            pass


# --------------------------------------------------
# Lancement de l’application
# --------------------------------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    if hasattr(Qt.ApplicationAttribute, "AA_EnableHighDpiScaling"):
        app.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())
