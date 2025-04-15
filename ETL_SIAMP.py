from gooey import Gooey, GooeyParser
import pandas as pd
import os
import re
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.utils import get_column_letter

@Gooey(
    program_name="Fusion Excel - SIAMP",
    language='french',
    default_size=(720, 640),
    navigation='TABBED',
    required_cols=1,
    optional_cols=2,
    show_success_modal=True,
    clear_before_run=True
)
def main():
    parser = GooeyParser(description="Fusionnez plusieurs fichiers Excel contenant des feuilles nommées Turnover")
    
    parser.add_argument(
        "fichiers",
        metavar="Fichiers Excel à fusionner",
        widget="MultiFileChooser",
        help="Sélectionnez les fichiers Excel à traiter (.xlsx uniquement)",
        nargs="+"
    )

    parser.add_argument(
        "nom_sortie",
        metavar="Nom du fichier de sortie",
        help="Nom du fichier final (ex: fusion.xlsx)",
        default="fusion_finale.xlsx"
    )

    args = parser.parse_args()

    # 🔎 Filtrage avancé pour exclure les fichiers Excel temporaires
    fichiers = [
        f for f in args.fichiers
        if f.endswith('.xlsx') and not os.path.basename(f).startswith('~$')
    ]
    
    nom_final = args.nom_sortie
    dossier_sortie = "output"
    os.makedirs(dossier_sortie, exist_ok=True)
    chemin_final = os.path.join(dossier_sortie, nom_final)

    # 📌 Regex pour identifier les feuilles "Turnover", "TURNOVER", ou "Turnover Oct 24"
    pattern_turnover = re.compile(r"^Turnover$|^TURNOVER$|^Turnover\s+[A-Z][a-z]{2}\s+\d{1,2}$")

    dfs = []

    for fichier in fichiers:
        try:
            xls = pd.ExcelFile(fichier, engine="openpyxl")
            feuilles = [s for s in xls.sheet_names if pattern_turnover.match(s)]

            if not feuilles:
                print(f"❌ Aucune feuille 'Turnover' valide trouvée dans {fichier}. Ignoré.")
                continue

            for feuille in feuilles:
                df = xls.parse(feuille, usecols="A:O")

                # ✅ Harmonisation des colonnes similaires
                if "CURRENCY" in df.columns and "Currency" not in df.columns:
                    df.rename(columns={"CURRENCY": "Currency"}, inplace=True)
                elif "Currency" in df.columns and "CURRENCY" in df.columns:
                    df["Currency"] = df["Currency"].combine_first(df["CURRENCY"])
                    df.drop(columns=["CURRENCY"], inplace=True)

                if "CUSTOMER NAME" in df.columns and "Customer" not in df.columns:
                    df.rename(columns={"CUSTOMER NAME": "Customer Name"}, inplace=True)
                elif "Customer" in df.columns and "CUSTOMER NAME" in df.columns:
                    df["Customer Name"] = df["Customer"].combine_first(df["CUSTOMER NAME"])
                    df.drop(columns=["Customer", "CUSTOMER NAME"], inplace=True)
                elif "Customer" in df.columns:
                    df.rename(columns={"Customer": "Customer Name"}, inplace=True)

                df["NomFichier"] = os.path.basename(fichier)
                df["Feuille"] = feuille
                dfs.append(df)

                print(f"✅ Feuille prise en compte : {feuille} ({os.path.basename(fichier)})")

        except Exception as e:
            print(f"❌ Erreur avec le fichier {fichier} : {e}")

    if not dfs:
        print("❌ Aucun fichier ou feuille valide n'a été trouvée.")
        return

    # 🔗 Fusion des données
    fusion = pd.concat(dfs, ignore_index=True)
    fusion.to_excel(chemin_final, index=False)

    # 🔽 Ajout automatique des filtres dans Excel via openpyxl
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

    wb.save(chemin_final)

    print(f"\n✅ Fusion réussie ! Fichier enregistré ici : {chemin_final}")

if __name__ == '__main__':
    main()
