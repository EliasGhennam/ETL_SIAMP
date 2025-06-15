# etl/date_utils.py
import pandas as pd
import re

def extract_year_from_filename(filename):
    match = re.search(r"20\d{2}", filename)
    return int(match.group()) if match else None

def format_date_column(df, year=None):
    if "MONTH" not in df.columns:
        return df

    df["DATE_TEMP"] = df["MONTH"].astype(str).str.strip()

    # Si le format est de type 1 à 12 et un an est fourni
    df["DATE_TEMP_NUM"] = pd.to_numeric(df["DATE_TEMP"], errors="coerce")
    if year is not None:
        valid_months = df["DATE_TEMP_NUM"].between(1, 12)
        df.loc[valid_months, "MONTH"] = pd.to_datetime(
            pd.to_datetime(f"{year}-01-01") + pd.to_timedelta((df["DATE_TEMP_NUM"] - 1) * 30, unit="D"),
            errors="coerce"
        )

    try:
        df["MONTH"] = pd.to_datetime(df["MONTH"], format="%d/%m/%Y", dayfirst=True, errors="coerce")
        if df["MONTH"].notna().sum() == 0:
            raise ValueError("Aucune date détectée")
    except:
        df["MONTH"] = pd.to_datetime(df["MONTH"], errors="coerce")

    return df

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



    # 🔍 Extraire les dates uniques de la colonne "MONTH"
    if "MONTH" in fusion.columns:
        try:
            fusion["MONTH"] = pd.to_datetime(fusion["MONTH"], errors="coerce")
            dates_disponibles = sorted(fusion["MONTH"].dropna().dt.strftime("%Y-%m-%d").unique())
        except Exception as e:
            print(f"[ERROR] Impossible de convertir les dates : {e}")
            dates_disponibles = []
    else:
        print("[WARN] ❌ Aucune colonne 'MONTH' trouvée.")
        dates_disponibles = []

    # 📋 Afficher les dates disponibles pour que l'utilisateur les choisisse
    if dates_disponibles:
        print(f"\n🗓️ Dates détectées dans les fichiers :\n" + "\n".join(f"  • {d}" for d in dates_disponibles))
        
        if args.mois_selectionnes:
            mois_choisis = args.mois_selectionnes.split(",")
            print(f"\n✅ Mois choisis via l'interface : {mois_choisis}")
            fusion = fusion[fusion["MONTH"].dt.to_period("M").astype(str).isin(mois_choisis)]
        else:
            if os.environ.get("FROM_GUI") == "1":
                print("[ERROR] ❌ Aucun mois sélectionné et interaction impossible (lancé depuis GUI). Merci de sélectionner les mois dans l’interface.")
                sys.exit(1)
            else:
                print("\n⏳ Entrez les dates à inclure séparées par une virgule (ex: 2025-01-01,2025-01-15) :")
                user_input = input(">>> ").strip()
                dates_choisies = [d.strip() for d in user_input.split(",") if d.strip() in dates_disponibles]
                print(f"\n✅ Dates retenues : {dates_choisies}\n")
                fusion = fusion[fusion["MONTH"].dt.strftime("%Y-%m-%d").isin(dates_choisies)]

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