#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ETL_SIAMP_GUI.py – Interface PyQt6 améliorée
----------------------------------------------
• Sélecteur de date + chargement historique des taux.
• Glisser‑déposer de fichiers Excel + ajout/retrait.
• Console en temps réel + barre de progression.
• Exécute le script core `ETL_SIAMP.py` via subprocess.
"""
from __future__ import annotations
import os
import sys
import re
import pandas as pd
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.utils import get_column_letter
import subprocess
import shutil
import calendar
from typing import List
import xml.etree.ElementTree as ET
from datetime import datetime
import requests
from PyQt6.QtCore   import Qt, QThread, pyqtSignal, QDate
from PyQt6.QtGui    import QIcon, QAction, QKeySequence, QPainter, QFont, QColor
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QPushButton, QFileDialog, QMessageBox, QListWidget, QComboBox,
    QPlainTextEdit, QProgressBar, QDateEdit, QInputDialog
)

SCRIPT_CORE = "ETL_SIAMP.py"
ICON_PATH   = "siamp_icon.ico"
CONFIG_FILE = "siamp_api_key.cfg"
DEFAULT_API = "tgogyMcj5vxTz5XDw9WDA90gYIueAV99IbgH"


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
        with open("error_log.txt", "w", encoding="utf-8", errors="replace") as err_file:
            proc = subprocess.Popen(
                self.cmd,
                stdout=subprocess.PIPE,
                stderr=err_file,
                text=True,
                env=self.env,
                encoding='utf-8',
                errors='replace'
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

    def __init__(self, on_click_callback=None):
        super().__init__()
        self.setAcceptDrops(True)
        self.setSelectionMode(self.SelectionMode.ExtendedSelection)
        self.setMinimumHeight(150)
        self.on_click_callback = on_click_callback  # fonction à appeler au clic

    def paintEvent(self, event):
        super().paintEvent(event)
        if self.count() == 0:
            painter = QPainter(self.viewport())
            painter.setPen(QColor("#777"))
            font = QFont("Segoe UI", 10, QFont.Weight.Normal)
            font.setItalic(True)
            painter.setFont(font)
            text = "Glissez vos fichiers Excel ici ou cliquez pour les sélectionner"
            painter.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, text)

    def mousePressEvent(self, event):
        if self.count() == 0 and self.on_click_callback:
            self.on_click_callback()  # déclenche la fonction ajout fichiers
        super().mousePressEvent(event)

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
        self._build_tabs()
        self._apply_style()

    def _detect_months(self):
        from collections import defaultdict
        from PyQt6.QtWidgets import QDialog, QTreeWidget, QTreeWidgetItem, QVBoxLayout, QPushButton

        mois_detectés = defaultdict(list)
        files = self.lst_files.files()

        if not files:
            QMessageBox.warning(self, "Erreur", "Ajoutez au moins un fichier Excel.")
            return

        # ➤ Détection des dates dans les fichiers
        for path in files:
            try:
                xls = pd.ExcelFile(path, engine="openpyxl")
                for sh in xls.sheet_names:
                    df = xls.parse(sh, usecols="A:Q")
                    df.columns = [c.strip().upper() for c in df.columns]
                    if "MONTH" in df.columns:
                        mois = pd.to_datetime(df["MONTH"], errors="coerce").dt.to_period("M")
                        mois_uniques = sorted(mois.dropna().unique())
                        for m in mois_uniques:
                            mois_detectés[str(m)].append(os.path.basename(path))
            except Exception as e:
                self.txt_log.appendPlainText(f"[WARN] ⚠ Fichier ignoré : {path} – {e}")

        if not mois_detectés:
            QMessageBox.information(self, "Info", "Aucune date détectée dans les fichiers.")
            return

        # ➤ Création de la boîte de dialogue
        dialog = QDialog(self)
        dialog.setWindowTitle("Sélectionnez les mois à traiter")
        layout = QVBoxLayout(dialog)
        tree = QTreeWidget()
        tree.setHeaderLabel("Mois détectés")
        tree.setColumnCount(1)
        tree.setSelectionMode(QTreeWidget.SelectionMode.MultiSelection)
        tree.setExpandsOnDoubleClick(True)

        # ➤ Construction de l'arborescence années/mois
        dates_groupées = defaultdict(set)
        for period in mois_detectés:
            annee, mois = period.split("-")
            dates_groupées[annee].add(mois)

        for annee, mois_set in sorted(dates_groupées.items()):
            parent = QTreeWidgetItem([annee])
            parent.setFlags(parent.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            parent.setCheckState(0, Qt.CheckState.Checked)
            for mois in sorted(mois_set):
                mois_int = int(mois)
                mois_nom = calendar.month_name[mois_int].capitalize()  # → "Février"
                child = QTreeWidgetItem([mois_nom])
                child.setFlags(child.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                child.setCheckState(0, Qt.CheckState.Checked)
                # ➡️ Important : stocker la vraie valeur numérique (ex. : "02") dans les "data"
                child.setData(0, Qt.ItemDataRole.UserRole, f"{int(mois):02d}")
                parent.addChild(child)
            tree.addTopLevelItem(parent)

        layout.addWidget(tree)

        btn_ok = QPushButton("Valider")
        btn_ok.clicked.connect(dialog.accept)
        layout.addWidget(btn_ok)

        dialog.exec()

        # ➤ Extraire les dates cochées
        dates_choisies = []
        for i in range(tree.topLevelItemCount()):
            parent = tree.topLevelItem(i)
            annee = parent.text(0)
            for j in range(parent.childCount()):
                child = parent.child(j)
                if child.checkState(0) == Qt.CheckState.Checked:
                    mois = child.data(0, Qt.ItemDataRole.UserRole)  # utilise le "data" plutôt que le texte affiché
                    dates_choisies.append(f"{annee}-{mois}")
        
        self.mois_selectionnes = dates_choisies  # Stocke la sélection pour l'utiliser dans _run_etl
        self.txt_log.appendPlainText(f"✅ Mois choisis : {self.mois_selectionnes}")

    def _build_tabs(self):
        from PyQt6.QtWidgets import QTabWidget

        self.tabs = QTabWidget()

        # Onglet 1 : Traitement mensuel (ce que tu avais déjà)
        self.page_traitement = QWidget()
        self.tabs.addTab(self.page_traitement, "Traitement Mensuel")
        self._build_traitement_ui(self.page_traitement)  # ⚠️ on utilise maintenant page_traitement ici

        # Onglet 2 : Fusion historique (nouvel onglet)
        self.page_historique = QWidget()
        self.tabs.addTab(self.page_historique, "Fusion Historique")
        self._build_historique_ui(self.page_historique)  # ⚠️ méthode à créer juste après
        self.setCentralWidget(self.tabs)

    def _build_historique_ui(self, parent_widget):
        layout = QVBoxLayout(parent_widget)

        # Fichiers historiques
        layout.addWidget(QLabel("Fichiers historiques à fusionner :"))
        self.lst_historique_files = DropListWidget(on_click_callback=self._add_historique_files)
        layout.addWidget(self.lst_historique_files)

        btn_bar = QHBoxLayout()
        btn_add = QPushButton("Ajouter…")
        btn_add.clicked.connect(self._add_historique_files)
        btn_bar.addWidget(btn_add)
        btn_rem = QPushButton("Retirer sélection")
        btn_rem.clicked.connect(self._remove_historique_files)
        btn_bar.addWidget(btn_rem)
        btn_bar.addStretch()
        layout.addLayout(btn_bar)
        self.lst_historique_files.setAlternatingRowColors(True)

        # Chemin de sortie
        row_out = QHBoxLayout()
        row_out.addWidget(QLabel("Fichier de sortie :"))
        self.txt_historique_out = QLineEdit("Historique_Consolide.xlsx")
        btn_out = QPushButton("Parcourir…")
        btn_out.clicked.connect(self._choose_historique_output)
        row_out.addWidget(self.txt_historique_out)
        row_out.addWidget(btn_out)
        layout.addLayout(row_out)

        # Barre de progression + bouton lancer
        self.pbar_historique = QProgressBar()
        self.pbar_historique.setMaximum(100)
        self.pbar_historique.setValue(0)
        layout.addWidget(self.pbar_historique)

        btn_run = QPushButton("▶ Fusionner l’historique")
        btn_run.setMinimumHeight(38)
        btn_run.clicked.connect(self._run_historique_fusion)
        layout.addWidget(btn_run)

        # Console historique
        self.txt_log_historique = QPlainTextEdit()
        self.txt_log_historique.setReadOnly(True)
        self.txt_log_historique.setMaximumBlockCount(1000)
        layout.addWidget(self.txt_log_historique, stretch=2)

    def _add_historique_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Sélectionner fichiers historiques", "", "Excel (*.xlsx)")
        for f in files:
            if f not in self.lst_historique_files.files():
                self.lst_historique_files.addItem(f)

    def _remove_historique_files(self):
        for item in self.lst_historique_files.selectedItems():
            self.lst_historique_files.takeItem(self.lst_historique_files.row(item))

    def _choose_historique_output(self):
        path, _ = QFileDialog.getSaveFileName(self, "Fichier de sortie historique", self.txt_historique_out.text(), "Excel (*.xlsx)")
        if path:
            self.txt_historique_out.setText(path)

    def _run_historique_fusion(self):
        files = self.lst_historique_files.files()
        if not files:
            return QMessageBox.warning(self, "Erreur", "Ajoutez au moins un fichier Excel à fusionner.")

        out = self.txt_historique_out.text().strip()
        if not out:
            return QMessageBox.warning(self, "Erreur", "Spécifiez le fichier de sortie.")

        try:
            self.txt_log_historique.clear()
            self.pbar_historique.setValue(0)
            all_dfs = []

            total = len(files)
            for idx, path in enumerate(files, 1):
                self.txt_log_historique.appendPlainText(f"[{idx}/{total}] Lecture : {os.path.basename(path)}")
                df = pd.read_excel(path, engine="openpyxl")
                all_dfs.append(df)
                self.pbar_historique.setValue(int((idx / total) * 100))
            
            if not all_dfs:
                self.txt_log_historique.appendPlainText("❌ Aucun fichier valide à fusionner.")
                return

            fusion = pd.concat(all_dfs, ignore_index=True)

            # Réordonner les colonnes comme dans l’ETL
            ORDER = [
                "MONTH", "SIAMP UNIT", "SALE TYPE", "TYPE OF CANAL", "ENSEIGNE", "CUSTOMER NAME",
                "COMMERCIAL AREA", "SUR FAMILLE", "FAMILLE", "REFERENCE", "PRODUCT NAME",
                "QUANTITY", "TURNOVER", "CURRENCY", "COUNTRY", "C.A en €",
                "VARIABLE COSTS", "COGS", "VAR Margin", "Margin", "NOMFICHIER", "FEUILLE"
            ]
            fusion = fusion[[c for c in ORDER if c in fusion.columns] +
                            [c for c in fusion.columns if c not in ORDER]]

            fusion.to_excel(out, index=False)

            # ➡️ ➕ Ajouter la mise en forme tableau avec filtres
            wb = load_workbook(out)
            ws = wb.active

            last_col_letter = get_column_letter(ws.max_column)
            last_row = ws.max_row
            table_range = f"A1:{last_col_letter}{last_row}"
            table = Table(displayName="HistoriqueTable", ref=table_range)
            table.tableStyleInfo = TableStyleInfo(
                name="TableStyleMedium9",
                showFirstColumn=False,
                showLastColumn=False,
                showRowStripes=True,
                showColumnStripes=False
            )
            ws.add_table(table)
            wb.save(out)

            self.txt_log_historique.appendPlainText(f"✅ Fusion terminée avec filtres activés. Fichier créé : {out}")
            self.pbar_historique.setValue(100)

        except Exception as e:
            QMessageBox.critical(self, "Erreur", f"Erreur durant la fusion : {e}")
            self.txt_log_historique.appendPlainText(f"[ERROR] {e}")
            self.pbar_historique.setValue(0)

    # ---------- UI construction ----------
    def _build_traitement_ui(self, parent_widget):
        layout = QVBoxLayout(parent_widget)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(12)

        # ► Sélecteur de date + bouton Charger taux
        row_date = QHBoxLayout()
        row_date.addWidget(QLabel("Date des taux :"))
        self.date_edit = QDateEdit(QDate.currentDate())
        self.date_edit.setCalendarPopup(True)
        row_date.addWidget(self.date_edit)
        btn_rates = QPushButton("Charger taux")
        btn_rates.clicked.connect(self._load_rates)
        row_date.addWidget(btn_rates)
        row_date.addStretch()
        layout.addLayout(row_date)

        # Taux manuel
        self.row_manual = QHBoxLayout()
        self.row_manual.addWidget(QLabel("Taux manuels (USD=0.93,GBP=1.15) :"))
        self.txt_manual = QLineEdit()
        self.row_manual.addWidget(self.txt_manual)
        layout.addLayout(self.row_manual)

        # Liste de fichiers
        layout.addWidget(QLabel("Fichiers Excel :"))
        self.lst_files = DropListWidget(on_click_callback=self._add_files)
        layout.addWidget(self.lst_files)

        # Boutons Ajouter / Retirer
        btn_bar = QHBoxLayout()
        btn_add = QPushButton("Ajouter…")
        btn_add.clicked.connect(self._add_files)
        btn_bar.addWidget(btn_add)
        btn_detect = QPushButton("🗓️ Détecter le ou les mois à traiter")
        btn_detect.clicked.connect(self._detect_months)
        btn_bar.addWidget(btn_detect)

        btn_rem = QPushButton("Retirer sélection")
        btn_rem.clicked.connect(self._remove_files)
        btn_bar.addWidget(btn_rem)
        btn_bar.addStretch()
        layout.addLayout(btn_bar)
        self.lst_files.setAlternatingRowColors(True)

        # Touche Suppr
        delete_act = QAction(
            self,
            shortcut=QKeySequence(Qt.Key.Key_Delete),
            triggered=self._remove_files
        )
        self.lst_files.addAction(delete_act)

        # Chemin de sortie
        row_out = QHBoxLayout()
        row_out.addWidget(QLabel("Fichier de sortie :"))
        self.txt_out = QLineEdit("fusion.xlsx")
        btn_out = QPushButton("Parcourir…")
        btn_out.clicked.connect(self._choose_output)
        row_out.addWidget(self.txt_out)
        row_out.addWidget(btn_out)
        layout.addLayout(row_out)

        # Barre de progression
        self.pbar = QProgressBar()
        self.pbar.setMaximum(100)
        self.pbar.setValue(0)
        layout.addWidget(self.pbar)

        # Bouton Lancer
        btn_run = QPushButton("▶ Lancer")
        btn_run.setMinimumHeight(38)
        btn_run.clicked.connect(self._run_etl)
        layout.addWidget(btn_run)

        # Console intégrée
        self.txt_log = QPlainTextEdit()
        self.txt_log.setReadOnly(True)
        self.txt_log.setMaximumBlockCount(1000)
        layout.addWidget(self.txt_log, stretch=2)


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
        # ➤ Détecter tous les mois distincts présents dans les fichiers
        from collections import defaultdict
        mois_detectés = defaultdict(list)

        for path in files:
            try:
                xls = pd.ExcelFile(path, engine="openpyxl")
                for sh in xls.sheet_names:
                    df = xls.parse(sh, usecols="A:Q")
                    df.columns = [c.strip().upper() for c in df.columns]
                    if "MONTH" in df.columns:
                        mois = pd.to_datetime(df["MONTH"], errors="coerce").dt.to_period("M")
                        mois_uniques = sorted(mois.dropna().unique())
                        for m in mois_uniques:
                            mois_detectés[str(m)].append(os.path.basename(path))
            except Exception as e:
                self.txt_log.appendPlainText(f"[WARN] ⚠ Fichier ignoré : {path} – {e}")

        if not files:
            return QMessageBox.warning(self, "Erreur", "Ajoutez au moins un fichier Excel.")
        out = self.txt_out.text().strip()
        if not out:
            return QMessageBox.warning(self, "Erreur", "Spécifiez le fichier de sortie.")

        man  = self.txt_manual.text().strip()
        if not man:
            self.txt_log.appendPlainText("💡 Aucun taux manuel saisi. Le programme utilisera uniquement les taux ECB.")

        # Chemin du script embarqué
        base_path = getattr(sys, '_MEIPASS', os.path.abspath("."))
        script_path = os.path.join(base_path, "ETL_SIAMP.py")

        # Trouve python.exe (depuis PATH ou venv)
        python_exe = shutil.which("python") or sys.executable

        cmd = [python_exe, script_path, "--chemin_sortie", out, "--fichiers", *files]
        if man:
            cmd += ["--taux_manuels", man]
        date_str = self.date_edit.date().toString("yyyy-MM-dd")
        cmd += ["--date", date_str]
        if hasattr(self, "mois_selectionnes") and self.mois_selectionnes:
            cmd += ["--mois_selectionnes", ",".join(self.mois_selectionnes)]


        env = dict(os.environ, GOOEY="0")

        self.txt_log.clear()
        self.pbar.setValue(0)

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

    def _load_rates(self):
        try:
            from datetime import datetime, timedelta
            from ETL_SIAMP import get_ecb_rates

            date = self.date_edit.date().toString("yyyy-MM-dd")
            limit_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=60)).strftime("%Y-%m-%d")
            rates = get_ecb_rates(date)

            # ➕ Ajouter manuellement les devises non couvertes par l'ECB
            rates.update({
                "MAD": 0.094,
                "TND": 0.30,
                "DZD": 0.0068,
                "XOF": 0.0015
            })

            # 🔎 Analyser les fichiers chargés pour détecter les devises utilisées
            devises_utilisées = set()
            TURNOVER_SHEET = re.compile(r"^TURNOVER($|\s+[A-Z][a-z]{2}\s+\d{1,2}$)", re.I)
            for i in range(self.lst_files.count()):
                path = self.lst_files.item(i).text()
                try:
                    xls = pd.ExcelFile(path, engine="openpyxl")
                    for sh in filter(TURNOVER_SHEET.match, xls.sheet_names):
                        df = xls.parse(sh, usecols="A:Q")
                        df.columns = [str(c).strip().upper() for c in df.columns]
                        if "CURRENCY" in df.columns:
                            devises_utilisées.update(df["CURRENCY"].dropna().astype(str).str.strip().str.upper())
                except Exception as e:
                    self.txt_log.appendPlainText(f"[WARN] ⚠ Impossible de lire {path} : {e}")

            # 🖨️ Affichage dans la console de l'UI
            self.txt_log.appendPlainText(f"📅 Taux de change ECB au {date} :\n")

            taux_manuels = self.txt_manual.text().strip()
            manuels = dict(part.split("=") for part in taux_manuels.split(",") if "=" in part)
            manuels = {k.strip().upper(): float(v) for k, v in manuels.items()}
            
            if not devises_utilisées:
                self.txt_log.appendPlainText("[INFO] Aucune devise détectée dans les fichiers, veuillez glisser déposer vos fichiers à traiter pour détécter les devises.\n")
            else:
                for cur in sorted(devises_utilisées):
                    if cur in rates:
                        self.txt_log.appendPlainText(f"  • {cur:<4} → {rates[cur]:.6f}")
                    elif cur in manuels:
                        self.txt_log.appendPlainText(f"  • {cur:<4} → {manuels[cur]:.6f} (manuel)")
                    else:
                        val, ok = QInputDialog.getDouble(
                            self, f"Taux manquant pour {cur}",
                            f"Aucun taux trouvé pour {cur}.\nEntrez le taux de conversion vers EUR :",
                            min=0.0001, decimals=6
                        )
                        if ok:
                            manuels[cur] = val
                            self.txt_log.appendPlainText(f"  • {cur:<4} → {val:.6f} (ajouté manuellement)")
                        else:
                            self.txt_log.appendPlainText(f"  • {cur:<4} → ❌ Non disponible")

                # Mise à jour du champ texte
                self.txt_manual.setText(",".join(f"{k}={v}" for k, v in manuels.items()))

        except Exception as e:
            QMessageBox.critical(self, "Erreur", f"Erreur lors de la récupération ECB :\n{e}")



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
