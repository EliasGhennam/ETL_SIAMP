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

def resource_path(relative_path: str) -> str:
    """Retourne le chemin absolu d'un fichier de ressources, compatible avec Nuitka (standalone ou non)."""
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)  # exécutable Nuitka
    return os.path.join(os.path.abspath("."), relative_path)  # mode normal


import re
import pandas as pd
import configparser
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
    QPlainTextEdit, QProgressBar, QDateEdit, QInputDialog, QFrame
)

SCRIPT_CORE = "ETL_SIAMP.py"
ICON_PATH        = resource_path("mydata/siamp_icon.ico")
CONFIG_FILE      = resource_path("mydata/siamp_api_key.cfg")
CONFIG_REF_FILE  = resource_path("mydata/ref_files.cfg")

# Définir un mapping de colonnes standard
COLUMN_MAPPING = {
    # Variations possibles -> Nom standardisé
    "MONTH": ["MONTH", "DATE", "PERIODE"],
    "CUSTOMER NAME": ["CUSTOMER NAME", "CUSTOMER", "CLIENT", "NOM CLIENT"],
    "REFERENCE": ["REFERENCE", "REF", "REFERENCE PRODUIT"],
    "TURNOVER": ["TURNOVER", "CA", "CHIFFRE D'AFFAIRE", "SALES"],
    "QUANTITY": ["QUANTITY", "QTY", "QUANTITE"],
    "CURRENCY": ["CURRENCY", "DEVISE", "MONNAIE"]
}

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

class ColumnStatusBar(QFrame):
    """Bandeau affichant le statut de détection des colonnes."""
    
    def __init__(self, expected_columns, parent=None):
        super().__init__(parent)
        self.expected_columns = expected_columns
        self.column_labels = {}
        self.setFrameStyle(QFrame.Shape.Panel | QFrame.Shadow.Sunken)
        
        # Layout horizontal avec scrolling si nécessaire
        layout = QHBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setSpacing(5)
        
        # Création des labels pour chaque colonne
        for col in expected_columns:
            label = QLabel(col)
            label.setFixedHeight(20)  # Plus petit
            label.setMaximumWidth(110)  # Largeur max, à ajuster selon ton UI
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            label.setWordWrap(False)
            label.setStyleSheet("""
                background-color: #3D444C;
                color: #AAAAAA; 
                border-radius: 4px;
                padding: 1px 6px;
                margin: 1px;
                font-size: 9pt;
            """)
            self.column_labels[col] = label
            layout.addWidget(label)

    def update_status_interactive(self, presence, all_files):
        for col, label in self.column_labels.items():
            files_with = presence.get(col, set())
            missing = [os.path.basename(f) for f in all_files if f not in files_with]
            if len(files_with) == len(all_files) and all_files:
                # Vert : présent partout
                label.setStyleSheet("background-color: #297F4F; color: white; border-radius: 4px; font-weight: bold; font-size:9pt;")
                label.setToolTip("Présent dans tous les fichiers")
                label.mousePressEvent = None
            elif len(files_with) == 0:
                # Rouge : absent partout
                label.setStyleSheet("background-color: #B22222; color: white; border-radius: 4px; font-weight: bold; font-size:9pt;")
                if missing:
                    label.setToolTip("Absent de tous les fichiers :\n" + "\n".join(missing))
                else:
                    label.setToolTip("Absent de tous les fichiers")
                label.mousePressEvent = None
            else:
                # Orange : partiel
                label.setStyleSheet("background-color: #FFA500; color: black; border-radius: 4px; font-weight: bold; font-size:9pt;")
                if missing:
                    label.setToolTip("Manquant dans :\n" + "\n".join(missing))
                else:
                    label.setToolTip("Présent partiellement")
                label.mousePressEvent = None  # On n'a plus besoin du clic


# ---------------------------------------------------------------- MainWindow
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ETL SIAMP — Fusion Excel")
        self.setWindowIcon(QIcon(ICON_PATH))
        self.resize(760, 640)
        self._build_tabs()
        self._apply_style()

    def _detect_months(self):
        from collections import defaultdict
        from PyQt6.QtWidgets import QDialog, QTreeWidget, QTreeWidgetItem, QVBoxLayout, QPushButton

        # Structure hiérarchique pour stocker année > mois > jour
        dates_hierarchie = defaultdict(lambda: defaultdict(set))
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
                        # Conversion robuste en dates
                        dates = pd.to_datetime(df["MONTH"], errors="coerce")
                        # Pour chaque date valide, extraire année/mois/jour
                        for date in dates.dropna():
                            dates_hierarchie[date.year][date.month].add(date.day)
            except Exception as e:
                self.txt_log.appendPlainText(f"[WARN] ⚠ Fichier ignoré : {path} – {e}")

        if not dates_hierarchie:
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

        # ➤ Construction de l'arborescence depuis notre hiérarchie
        for annee in sorted(dates_hierarchie.keys()):
            parent = QTreeWidgetItem([str(annee)])
            parent.setFlags(parent.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            parent.setCheckState(0, Qt.CheckState.Checked)
            
            for mois_num in sorted(dates_hierarchie[annee].keys()):
                mois_nom = calendar.month_name[mois_num].capitalize()
                child = QTreeWidgetItem([mois_nom])
                child.setFlags(child.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                child.setCheckState(0, Qt.CheckState.Checked)
                # Stocke le numéro du mois formaté avec leading zero
                child.setData(0, Qt.ItemDataRole.UserRole, f"{mois_num:02d}")
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
                    mois = child.data(0, Qt.ItemDataRole.UserRole)
                    dates_choisies.append(f"{annee}-{mois}")
        
        self.mois_selectionnes = dates_choisies
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

        # Onglet 3 : Paramètres (NOUVEAU)
        self.page_parametres = QWidget()
        self.tabs.addTab(self.page_parametres, "Paramètres / Références")
        self._build_parametres_ui(self.page_parametres)  # 👈 à créer juste après

    def _build_historique_ui(self, parent_widget):
        layout = QVBoxLayout(parent_widget)

        # Fichiers historiques
        layout.addWidget(QLabel("Fichiers historiques à fusionner :"))
        self.lst_historique_files = DropListWidget(on_click_callback=self._add_historique_files)
        layout.addWidget(self.lst_historique_files)

        btn_bar = QHBoxLayout()
        btn_add = QPushButton("Ajouter…")
        btn_add.clicked.connect(self._add_historique_files)
        btn_rem = QPushButton("Retirer sélection")
        btn_rem.clicked.connect(self._remove_historique_files)
        btn_bar.addWidget(btn_add)
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

        btn_run = QPushButton("▶ Fusionner l'historique")
        btn_run.setMinimumHeight(38)
        btn_run.clicked.connect(self._run_historique_fusion)
        layout.addWidget(btn_run)

        # Console historique
        self.txt_log_historique = QPlainTextEdit()
        self.txt_log_historique.setReadOnly(True)
        self.txt_log_historique.setMaximumBlockCount(1000)
        layout.addWidget(self.txt_log_historique, stretch=2)

        # Bandeau des colonnes historiques
        layout.addWidget(QLabel("<b>Colonnes attendues (historique) :</b>"))
        expected_histo_columns = [
            "MONTH", "SIAMP UNIT", "SALE TYPE", "TYPE OF CANAL", "CUSTOMER NAME", "COMMERCIAL AREA",
            "SUR FAMILLE", "FAMILLE", "REFERENCE", "PRODUCT NAME", "QUANTITY", "TURNOVER", "CURRENCY",
            "COUNTRY", "C.A en €", "VARIABLE COSTS", "COGS", "VAR Margin", "Margin",
            "NOMFICHIER", "FEUILLE", "Enseigne ret", "Surfamille"
        ]
        self.histo_column_status_bar = ColumnStatusBar(expected_histo_columns)
        layout.addWidget(self.histo_column_status_bar)

        btn_check_histo = QPushButton("Vérifier le contenu")
        btn_check_histo.clicked.connect(self._check_histo_columns_in_files)
        layout.addWidget(btn_check_histo)

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

    def _extract_year_from_filename(self, filename):
        """Extrait l'année du nom du fichier (ex: STATS 2024.xlsx -> 2024)"""
        match = re.search(r'20\d{2}', filename)
        if match:
            return int(match.group())
        return None

    def _format_date_column(self, df, year=None):
        """Formate la colonne MONTH en gérant les différents formats de date possibles"""
        if "MONTH" not in df.columns:
            return df

        # Créer une copie de la colonne pour préserver les données originales
        df["DATE_TEMP"] = df["MONTH"].astype(str)

        # 1. D'abord essayer de parser comme date complète
        try:
            dates = pd.to_datetime(df["DATE_TEMP"], format='%d/%m/%Y', errors='coerce')
            mask_fr = dates.notna()
            if mask_fr.any():
                df.loc[mask_fr, "DATE_TEMP"] = dates[mask_fr].dt.strftime("%d/%m/%Y")
        except:
            pass

        try:
            dates = pd.to_datetime(df["DATE_TEMP"], errors='coerce')
            mask_other = dates.notna()
            if mask_other.any():
                df.loc[mask_other, "DATE_TEMP"] = dates[mask_other].dt.strftime("%d/%m/%Y")
        except:
            pass

        # 2. Pour les valeurs qui sont des nombres (incluant les décimales), convertir en date avec l'année du fichier
        if year:
            # Convertir en numérique et arrondir pour gérer les .0 ou .00
            df["DATE_TEMP_NUM"] = pd.to_numeric(df["DATE_TEMP"], errors='coerce')
            numeric_mask = df["DATE_TEMP_NUM"].notna()
            if numeric_mask.any():
                try:
                    # Arrondir et vérifier si dans la plage 1-12
                    df.loc[numeric_mask, "DATE_TEMP_NUM"] = df.loc[numeric_mask, "DATE_TEMP_NUM"].round()
                    valid_months = (df["DATE_TEMP_NUM"] >= 1) & (df["DATE_TEMP_NUM"] <= 12)
                    if valid_months.any():
                        df.loc[valid_months, "DATE_TEMP"] = pd.to_datetime(
                            df.loc[valid_months, "DATE_TEMP_NUM"].apply(
                                lambda x: f"01/{int(x):02d}/{year}"
                            ),
                            format="%d/%m/%Y"
                        ).dt.strftime("%d/%m/%Y")
                except:
                    pass
            
            df = df.drop(columns=["DATE_TEMP_NUM"])

        # Remplacer l'ancienne colonne MONTH
        df["MONTH"] = df["DATE_TEMP"]
        df = df.drop(columns=["DATE_TEMP"])
        return df

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
                
                # Extraire l'année du nom de fichier
                year = self._extract_year_from_filename(os.path.basename(path))
                
                # Formater les dates
                df = self._format_date_column(df, year)
                
                all_dfs.append(df)
                self.pbar_historique.setValue(int((idx / total) * 100))
            
            if not all_dfs:
                self.txt_log_historique.appendPlainText("❌ Aucun fichier valide à fusionner.")
                return

            fusion = pd.concat(all_dfs, ignore_index=True)

            # Réordonner les colonnes comme dans l'ETL
            ORDER = [
                "MONTH", "SIAMP UNIT", "SALE TYPE", "TYPE OF CANAL", "ENSEIGNE", "CUSTOMER NAME",
                "COMMERCIAL AREA", "SUR FAMILLE", "FAMILLE", "REFERENCE", "PRODUCT NAME",
                "QUANTITY", "TURNOVER", "CURRENCY", "COUNTRY", "C.A en €",
                "VARIABLE COSTS", "COGS", "VAR Margin", "Margin", "SOURCE", "NOMFICHIER", "FEUILLE"
            ]
            fusion = fusion[[c for c in ORDER if c in fusion.columns] +
                            [c for c in fusion.columns if c not in ORDER]]
            
            # ➤ Réorganisation des colonnes dans l’ordre métier
            fusion = fusion[[c for c in ORDER if c in fusion.columns]
                            + [c for c in fusion.columns if c not in ORDER]]

            # ➤ Supprimer les doublons métier APRÈS enrichissement
            colonnes_cle = ["MONTH", "REFERENCE", "CUSTOMER NAME", "QUANTITY"]
            before = fusion.shape[0]
            fusion = fusion.drop_duplicates(subset=colonnes_cle, keep="last")
            after = fusion.shape[0]
            print(f"[INFO] 🧹 {before - after} doublon(s) supprimé(s) après enrichissements", flush=True)

            # ➤ Sauvegarde Excel
            fusion.to_excel(out, index=False)


            # Sauvegarder en Excel
            fusion.to_excel(out, index=False)

            # Appliquer le formatage Excel
            wb = load_workbook(out)
            ws = wb.active

            # Définir la plage du tableau et créer une table formatée
            last_col_letter = get_column_letter(ws.max_column)
            last_row = ws.max_row
            table_range = f"A1:{last_col_letter}{last_row}"

            # Créer et appliquer la table avec style
            table = Table(displayName="HistoriqueTable", ref=table_range)
            table.tableStyleInfo = TableStyleInfo(
                name="TableStyleMedium2",
                showFirstColumn=False,
                showLastColumn=False,
                showRowStripes=True,
                showColumnStripes=False
            )
            
            # Supprimer toute table existante et ajouter la nouvelle
            ws._tables.clear()
            ws.add_table(table)

            # Formater les colonnes spécifiques
            for idx, column in enumerate(ws[1], 1):
                col_letter = get_column_letter(idx)
                
                # Formater la colonne MONTH comme date
                if column.value == "MONTH":
                    for cell in ws[col_letter][1:]:  # Skip header
                        if cell.value:
                            try:
                                if isinstance(cell.value, (datetime, pd.Timestamp)):
                                    # Déjà en datetime, appliquer juste le format
                                    cell.number_format = "dd/mm/yyyy"
                                else:
                                    # Convertir en datetime Excel
                                    date_val = pd.to_datetime(cell.value, errors="coerce")
                                    if pd.notna(date_val):
                                        cell.value = date_val
                                        cell.number_format = "dd/mm/yyyy"
                            except Exception:
                                pass

                # Formater uniquement la colonne "TURNOVER €" avec le symbole €
                elif column.value == "TURNOVER €":
                    for cell in ws[col_letter][1:]:
                        if cell.value and isinstance(cell.value, (int, float)):
                            cell.number_format = "#,##0.00 €"

                # Formater les autres colonnes monétaires sans le symbole €
                elif column.value in ["TURNOVER", "C.A en €", "VARIABLE COSTS", "COGS", "VAR Margin", "Margin"]:
                    for cell in ws[col_letter][1:]:
                        if cell.value and isinstance(cell.value, (int, float)):
                            cell.number_format = "#,##0.00"

                # Formater la colonne QUANTITY
                elif column.value == "QUANTITY":
                    for cell in ws[col_letter][1:]:
                        if cell.value and isinstance(cell.value, (int, float)):
                            cell.number_format = "#,##0"

            # Figer la première ligne
            ws.freeze_panes = "A2"

            # Ajuster la largeur des colonnes
            for column in ws.columns:
                max_length = 0
                column_letter = get_column_letter(column[0].column)
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                ws.column_dimensions[column_letter].width = min(adjusted_width, 50)

            wb.save(out)
            self.txt_log_historique.appendPlainText(f"✅ Fusion terminée avec mise en forme optimisée. Fichier créé : {out}")
            self.pbar_historique.setValue(100)
        except Exception as e:
            self.txt_log_historique.appendPlainText(f"[ERROR] ❌ Une erreur est survenue pendant la fusion : {e}")
            import traceback
            traceback.print_exc()

    def _check_histo_columns_in_files(self):
        files = self.lst_historique_files.files()
        expected = self.histo_column_status_bar.expected_columns
        presence = {col: set() for col in expected}
        for path in files:
            try:
                xls = pd.ExcelFile(path, engine="openpyxl")
                for sh in xls.sheet_names:
                    df = pd.read_excel(path, sheet_name=sh, nrows=0)
                    cols = [c.strip().upper() for c in df.columns]
                    for col in expected:
                        if col in cols:
                            presence[col].add(path)
            except Exception as e:
                self.txt_log_historique.appendPlainText(f"[WARN] ⚠ Fichier ignoré dans la détection : {path} – {e}")
        self.histo_column_status_bar.update_status_interactive(presence, files)

    # ---------- UI construction ----------
    def _build_traitement_ui(self, parent_widget):
        layout = QVBoxLayout(parent_widget)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(12)
        
        # Colonnes attendues
        expected_columns = [
            "MONTH", "SIAMP UNIT", "SALE TYPE", "TYPE OF CANAL", "ENSEIGNE", 
            "CUSTOMER NAME", "COMMERCIAL AREA", "SUR FAMILLE", "FAMILLE", 
            "REFERENCE", "PRODUCT NAME", "QUANTITY", "TURNOVER", 
            "CURRENCY", "COUNTRY", "VARIABLE COSTS", "COGS"
        ]
        
        # Bandeau des colonnes
        layout.addWidget(QLabel("<b>Colonnes attendues :</b>"))
        self.column_status_bar = ColumnStatusBar(expected_columns)
        layout.addWidget(self.column_status_bar)
        
        btn_check = QPushButton("Vérifier le contenu")
        btn_check.clicked.connect(self._check_columns_in_files)
        layout.addWidget(btn_check)
        
        # ► Sélecteur de date + bouton Charger taux
        row_date = QHBoxLayout()
        row_date.addWidget(QLabel("Date des taux :"))
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
        self.row_manual.addWidget(QLabel("Taux manuels (USD=0.93,GBP=1.15) :"))
        self.txt_manual = QLineEdit()
        self.row_manual.addWidget(self.txt_manual)
        layout.addLayout(self.row_manual)

        # Liste de fichiers
        layout.addWidget(QLabel("Fichiers Excel :"))
        self.lst_files = DropListWidget(on_click_callback=self._add_files)
        layout.addWidget(self.lst_files)

        # Boutons Ajouter / Retirer
        btn_bar = QHBoxLayout()
        btn_add = QPushButton("Ajouter…")
        btn_add.clicked.connect(self._add_files)
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
        row_out.addWidget(QLabel("Fichier de sortie :"))
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
            "Traitement terminé avec succès !" if ok else "Le script a échoué."
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


    def _build_parametres_ui(self, parent_widget):
        layout = QVBoxLayout(parent_widget)

        layout.addWidget(QLabel("Sélection des fichiers de référence :"))

        # Fichier ZONE AFFECTATION
        row_zone = QHBoxLayout()
        row_zone.addWidget(QLabel("ZONE AFFECTATION :"))
        self.txt_zone_affectation = QLineEdit()
        btn_zone = QPushButton("Parcourir…")
        btn_zone.clicked.connect(self._choose_zone_affectation)
        row_zone.addWidget(self.txt_zone_affectation)
        row_zone.addWidget(btn_zone)
        layout.addLayout(row_zone)

        # Fichier table
        row_table = QHBoxLayout()
        row_table.addWidget(QLabel("table :"))
        self.txt_table_file = QLineEdit()
        btn_table = QPushButton("Parcourir…")
        btn_table.clicked.connect(self._choose_table_file)
        row_table.addWidget(self.txt_table_file)
        row_table.addWidget(btn_table)
        layout.addLayout(row_table)

        # Bouton Sauvegarder
        btn_save = QPushButton("💾 Sauvegarder les chemins")
        btn_save.clicked.connect(self._save_reference_paths)
        layout.addWidget(btn_save)

        # Charger si config existe
        self._load_reference_paths()

    def _save_reference_paths(self):
        config = configparser.ConfigParser()
        config['REFERENCES'] = {
            'zone_affectation': self.txt_zone_affectation.text(),
            'table': self.txt_table_file.text()
        }
        
        # Créer le répertoire mydata s'il n'existe pas
        config_dir = os.path.dirname(CONFIG_REF_FILE)
        os.makedirs(config_dir, exist_ok=True)
        
        with open(CONFIG_REF_FILE, 'w') as cfgfile:
            config.write(cfgfile)
        QMessageBox.information(self, "Succès", "Les chemins des fichiers de référence ont été sauvegardés.")
        self._load_reference_paths()  # Recharge directement après sauvegarde

    def _load_reference_paths(self):
        if os.path.exists(CONFIG_REF_FILE):
            config = configparser.ConfigParser()
            config.read(CONFIG_REF_FILE)
            refs = config['REFERENCES']
            self.txt_zone_affectation.setText(refs.get('zone_affectation', ''))
            self.txt_table_file.setText(refs.get('table', ''))

            # ✅ Check si les fichiers existent physiquement
            zone_ok = os.path.exists(self.txt_zone_affectation.text())
            table_ok = os.path.exists(self.txt_table_file.text())

            if not zone_ok or not table_ok:
                msg = "⚠️ Fichiers de référence manquants ou invalides :\n"
                if not zone_ok:
                    msg += f" - ZONE AFFECTATION : {self.txt_zone_affectation.text()}\n"
                if not table_ok:
                    msg += f" - table : {self.txt_table_file.text()}\n"
                QMessageBox.warning(self, "Attention", msg)
                
    def _choose_zone_affectation(self):
        path, _ = QFileDialog.getOpenFileName(self, "Choisir ZONE AFFECTATION", "", "Excel (*.xlsx)")
        if path:
            try:
                xls = pd.ExcelFile(path, engine="openpyxl")
                if "ZONE AFFECTATION" in xls.sheet_names:
                    self.txt_zone_affectation.setText(path)
                    QMessageBox.information(self, "✅ Succès", f"Fichier validé : Feuille 'ZONE AFFECTATION' détectée.")
                else:
                    QMessageBox.warning(self, "Erreur", f"Aucune feuille 'ZONE AFFECTATION' trouvée dans ce fichier.")
            except Exception as e:
                QMessageBox.critical(self, "Erreur", f"Impossible d'ouvrir le fichier :\n{e}")


    def _choose_table_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Choisir table", "", "Excel (*.xlsx)")
        if path:
            try:
                xls = pd.ExcelFile(path, engine="openpyxl")
                if "table" in xls.sheet_names:
                    self.txt_table_file.setText(path)
                    QMessageBox.information(self, "✅ Succès", f"Fichier validé : Feuille 'table' détectée.")
                else:
                    QMessageBox.warning(self, "Erreur", f"Aucune feuille 'table' trouvée dans ce fichier.")
            except Exception as e:
                QMessageBox.critical(self, "Erreur", f"Impossible d'ouvrir le fichier :\n{e}")

    def _check_columns_in_files(self):
        files = self.lst_files.files()
        expected = self.column_status_bar.expected_columns
        # Dictionnaire : colonne -> set des fichiers où elle est présente
        presence = {col: set() for col in expected}
        for path in files:
            try:
                xls = pd.ExcelFile(path, engine="openpyxl")
                for sh in xls.sheet_names:
                    df = pd.read_excel(path, sheet_name=sh, nrows=0)
                    cols = [c.strip().upper() for c in df.columns]
                    for col in expected:
                        if col in cols:
                            presence[col].add(path)
            except Exception as e:
                self.txt_log.appendPlainText(f"[WARN] ⚠ Fichier ignoré dans la détection : {path} – {e}")
        self.column_status_bar.update_status_interactive(presence, files)


# --------------------------------------------------
# Lancement de l'application
# --------------------------------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    if hasattr(Qt.ApplicationAttribute, "AA_EnableHighDpiScaling"):
        app.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())

def normalize_columns(df):
    normalized_columns = df.columns.copy()
    for standard_name, variations in COLUMN_MAPPING.items():
        for col in df.columns:
            if col.strip().upper() in [v.strip().upper() for v in variations]:
                normalized_columns = normalized_columns.str.replace(col, standard_name)
    df.columns = normalized_columns
    return df

def clean_and_validate_data(df):
    # Conversion des dates
    if "MONTH" in df.columns:
        df["MONTH"] = pd.to_datetime(df["MONTH"], errors="coerce")
    
    # Nettoyage des valeurs numériques
    numeric_columns = ["TURNOVER", "QUANTITY"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Standardisation des devises
    if "CURRENCY" in df.columns:
        df["CURRENCY"] = df["CURRENCY"].str.strip().str.upper()
    
    return df

def identify_merge_keys(df):
    potential_keys = ["REFERENCE", "CUSTOMER NAME", "MONTH"]
    available_keys = [key for key in potential_keys if key in df.columns]
    return available_keys

def format_month_column(df, year=None):
    """Formate correctement la colonne MONTH en gérant différents formats."""
    if "MONTH" not in df.columns:
        return df

    # Si valeurs entre 1-12 et année fournie => convertir en date complète
    numeric_months = pd.to_numeric(df["MONTH"], errors="coerce")
    month_mask = numeric_months.between(1, 12)
    if year and month_mask.any():
        df.loc[month_mask, "MONTH"] = pd.to_datetime(
            [f"{year}-{int(m):02d}-01" for m in numeric_months[month_mask]]
        )

    # Essai de conversion complète en date
    df["MONTH"] = pd.to_datetime(df["MONTH"], errors="coerce", dayfirst=True)
    
    return df
