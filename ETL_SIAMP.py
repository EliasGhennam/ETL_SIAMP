# -*- coding: utf-8 -*-
import pandas as pd
import os
import re
import glob
from time import sleep
import warnings
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.utils import get_column_letter
from openpyxl.styles import numbers
from openpyxl.styles.numbers import BUILTIN_FORMATS
import sys
import io
import argparse

# Console UTF-8
if sys.stdout and hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Taux de conversion fixes (exemple, ajustables)
taux_conversion = {
    "EUR": 1.0,
    "USD": 0.93,
    "GBP": 1.15,
    "EGP": 0.03,
    "CHF": 1.04,
    "AED": 0.25,
    "JPY": 0.0062
}

def main():
    parser = argparse.ArgumentParser(description="Fusionnez plusieurs fichiers Excel contenant des feuilles nommées Turnover")
    parser.add_argument("--fichiers", nargs='+', required=True, help="Liste des fichiers Excel à fusionner (peut inclure des jokers, ex: fichiers_excel/*.xlsx)")
    parser.add_argument("--chemin_sortie", required=True, help="Chemin du fichier Excel final")
    args = parser.parse_args()

    fichiers = []
    for path in args.fichiers:
        fichiers.extend(glob.glob(path))

    fichiers = [
        f for f in fichiers
        if f.endswith('.xlsx') and not os.path.basename(f).startswith('~$')
    ]

    chemin_final = args.chemin_sortie
    dossier_sortie = os.path.dirname(chemin_final)

    if not dossier_sortie:
        dossier_sortie = "."
        chemin_final = os.path.join(dossier_sortie, chemin_final)

    os.makedirs(dossier_sortie, exist_ok=True)

    if not chemin_final.lower().endswith(".xlsx"):
        chemin_final += ".xlsx"

    pattern_turnover = re.compile(r"^Turnover$|^TURNOVER$|^Turnover\s+[A-Z][a-z]{2}\s+\d{1,2}$")
    dfs = []
    total = len(fichiers)

    print("Début de la fusion des fichiers...\n")

    for i, fichier in enumerate(fichiers):
        print(f"🔍 Analyse du fichier : {os.path.basename(fichier)}", flush=True)
        try:
            xls = pd.ExcelFile(fichier, engine="openpyxl")
            feuilles = [s for s in xls.sheet_names if pattern_turnover.match(s)]

            if not feuilles:
                print(f"⚠️ Aucune feuille Turnover détectée dans {os.path.basename(fichier)}. Vérifiez son format.", flush=True)
                continue

            for feuille in feuilles:
                print(f"✅ Feuille trouvée : {feuille} ({os.path.basename(fichier)})", flush=True)
                df = xls.parse(feuille, usecols="A:O")

                df.dropna(axis=1, how="all", inplace=True)

                df.columns = [col.strip().upper() for col in df.columns]
                rename_dict = {}
                for col in df.columns:
                    if col == "CURRENCY":
                        rename_dict[col] = "Currency"
                    elif col == "TURNOVER":
                        rename_dict[col] = "TURNOVER"
                    elif col == "CUSTOMER NAME":
                        rename_dict[col] = "Customer Name"
                    elif col == "CUSTOMER":
                        rename_dict[col] = "Customer Name"
                df.rename(columns=rename_dict, inplace=True)

                turnover_col = "TURNOVER" if "TURNOVER" in df.columns else None
                quantity_col = "QUANTITY" if "QUANTITY" in df.columns else None
                if turnover_col or quantity_col:
                    df = df[~(df.get(turnover_col).isna() & df.get(quantity_col).isna())]

                if "Currency" in df.columns:
                    df["Currency"] = df["Currency"].fillna(method="ffill")

                if "Currency" in df.columns and "TURNOVER" in df.columns:
                    def conversion(row):
                        devise = str(row["Currency"]).strip().upper()
                        montant = row["TURNOVER"]
                        if pd.isna(montant):
                            return None
                        if not devise or devise == "NAN":
                            print(f"[AVERTISSEMENT] Aucune devise indiquée pour une ligne de {os.path.basename(fichier)}", flush=True)
                            return None
                        taux = taux_conversion.get(devise)
                        if taux:
                            return round(montant * taux, 2)
                        else:
                            print(f"[AVERTISSEMENT] Devise inconnue '{devise}' dans {os.path.basename(fichier)}", flush=True)
                            return None

                    df.insert(df.columns.get_loc("TURNOVER") + 1, "C.A en €", df.apply(conversion, axis=1))

                
                # Suppression des lignes avec TURNOVER ou QUANTITY non numériques
                colonnes_a_verifier = [col for col in ["TURNOVER", "QUANTITY"] if col in df.columns]
                if colonnes_a_verifier:
                    masque = pd.Series(True, index=df.index)
                    for col in colonnes_a_verifier:
                        masque &= pd.to_numeric(df[col], errors="coerce").notna()
                    lignes_supprimees = (~masque).sum()
                    if lignes_supprimees > 0:
                        print(f"[INFO] {lignes_supprimees} ligne(s) supprimée(s) pour valeurs non numériques dans {', '.join(colonnes_a_verifier)}.", flush=True)
                    df = df[masque]

                    df["NomFichier"] = os.path.basename(fichier)
                    df["Feuille"] = feuille
                    dfs.append(df)


                print(f"[OK] Feuille ajoutée : {feuille} ({os.path.basename(fichier)})")

        except Exception as e:
            print(f"[ERREUR] Problème avec {fichier} : {e}")

        pourcentage = int(((i + 1) / total) * 100)
        print(f"PROGRESS: {pourcentage}%", flush=True)

        if pourcentage == 100:
            print("\n⏳ Les données sont entièrement chargées. Veuillez patienter pendant la finalisation du fichier Excel (ne fermez pas l'application)...", flush=True)

        sleep(0.1)

    if not dfs:
        print("\nAucun fichier ou feuille valide détecté. Arrêt.")
        return

    fusion = pd.concat(dfs, ignore_index=True)
    fusion.to_excel(chemin_final, index=False)

    wb = load_workbook(chemin_final)
    ws = wb.active
    max_col = ws.max_column
    max_row = ws.max_row
    end_col_letter = get_column_letter(max_col)
    table_range = f"A1:{end_col_letter}{max_row}"

    table = Table(displayName="FusionTable", ref=table_range)
    style = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
    table.tableStyleInfo = style
    ws.add_table(table)

    # ✅ Appliquer le bon format monétaire à la colonne "C.A en €" avec le symbole € aligné à droite
    for col in ws.iter_cols(min_row=2, max_row=max_row):
        header_cell = ws[f"{col[0].column_letter}1"]
        if header_cell.value == "C.A en €":
            for cell in col:
                cell.number_format = u"#,##0.00\u00a0€"

    wb.save(chemin_final)

    recap = "\n=== ✅ FUSION COMPLÉTÉE AVEC SUCCÈS ===\n"
    recap += f"📄 Fichier généré : {chemin_final}\n"
    recap += "\nMerci d’avoir utilisé l’outil ETL SIAMP. 🚀\n"

    print(recap, flush=True)

if __name__ == '__main__':
    main()