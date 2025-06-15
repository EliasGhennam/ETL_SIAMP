import pandas as pd
import os
from etl.rates import get_ecb_rates
from etl.enrich import enrich_dataframe
from etl.date_utils import format_date_column

def main():
    parser = argparse.ArgumentParser(description="Fusionnez plusieurs fichiers Excel Turnover")
    parser.add_argument("--fichiers",      nargs='+', required=True)
    parser.add_argument("--chemin_sortie", required=True)
    parser.add_argument("--taux_manuels",  help="USD=0.93,GBP=1.15", default=None)
    parser.add_argument("--date",          help="YYYY-MM-DD pour historique (premium)", default=None)
    parser.add_argument("--date_debut", help="Date début de la période à filtrer (YYYY-MM-DD)", default=None)
    parser.add_argument("--date_fin",   help="Date fin de la période à filtrer (YYYY-MM-DD)", default=None)
    parser.add_argument("--mois_selectionnes", help="Liste des mois à traiter, séparés par des virgules (ex: 2025-02,2025-03)", default=None)

    args = parser.parse_args()
    # ----------------------------------------- Charger les chemins des fichiers de référence
    CONFIG_REF_FILE = "ref_files.cfg"
    zone_affectation_path = None
    table_path = None

    if os.path.exists(CONFIG_REF_FILE):
        config = configparser.ConfigParser()
        config.read(CONFIG_REF_FILE)
        refs = config['REFERENCES']
        zone_affectation_path = refs.get('zone_affectation', None)
        table_path = refs.get('table', None)
    else:
        print("[WARN] ⚠️ Fichier de config 'ref_files.cfg' introuvable. Les colonnes de correspondance ne seront pas alimentées.")


    devises_detectées: set[str] = set()

    print(f"[DEBUG] 👋 Script lancé avec date = {args.date}", flush=True)

    # parse manuels
    manu: dict[str,float] = {}
    if args.taux_manuels:
        for part in args.taux_manuels.split(","):
            try:
                c,v = part.split("=")
                manu[c.strip().upper()] = float(v)
            except:
                print(f"[WARN] taux manuel ignoré: {part}", flush=True)

    # collecte fichiers
    files: list[str] = []
    for patt in args.fichiers:
        files.extend(glob.glob(patt))
    files = [f for f in files if f.lower().endswith(".xlsx")
             and not os.path.basename(f).startswith("~$")]
    if not files:
        sys.exit("Aucun fichier .xlsx trouvé.")

    out = args.chemin_sortie
    if not out.lower().endswith(".xlsx"):
        out += ".xlsx"
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)

    # patterns
    TURNOVER_SHEET = re.compile(r"^TURNOVER($|\s+[A-Z][a-z]{2}\s+\d{1,2}$)", re.I)
    VAR_PATTS  = [r"^CD\s*\+\s*FSD", r"^CD\+FSD", r"^VARIABLE\s*COSTS?"]
    COGS_PATTS = [r"^PRU", r"^COGS"]

    all_dfs: list[pd.DataFrame] = []
    total = len(files)
    for idx, path in enumerate(files, 1):
        print(f"[{idx}/{total}] {os.path.basename(path)}", flush=True)
        try:
            xls = pd.ExcelFile(path, engine="openpyxl")
            for sh in filter(TURNOVER_SHEET.match, xls.sheet_names):
                df = xls.parse(sh, usecols="A:Q")
                df.dropna(axis=1, how="all", inplace=True)
                df.columns = [c.strip() for c in df.columns]

                # renommage
                ren: dict[str,str] = {}
                for c in df.columns:
                    U = c.upper()
                    if any(re.match(p,U) for p in VAR_PATTS):
                        ren[c] = "VARIABLE COSTS"
                    elif any(re.match(p,U) for p in COGS_PATTS):
                        ren[c] = "COGS"
                    elif U=="TURNOVER":
                        ren[c] = "TURNOVER"
                    elif U=="CURRENCY":
                        ren[c] = "CURRENCY"
                    elif U in {"CUSTOMER","CUSTOMER NAME"}:
                        ren[c] = "CUSTOMER NAME"
                df.rename(columns=ren, inplace=True)

                print("    -> Colonnes:", ", ".join(df.columns), flush=True)

                # log var/cogs
                for nm in ("VARIABLE COSTS","COGS"):
                    if nm in df.columns:
                        n = df[nm].notna().sum()
                        print(f"       • {nm} détectée: {n} valeurs non-null", flush=True)

                df["NOMFICHIER"] = os.path.basename(path)
                df["FEUILLE"]     = sh
                # Conversion explicite de la première colonne (MONTH) en datetime si possible
                if "MONTH" in df.columns:
                    try:
                        df["MONTH"] = pd.to_datetime(df["MONTH"], errors="coerce")
                        nb_dates = df["MONTH"].notna().sum()
                        print(f"       📅 Dates valides détectées dans 'MONTH' : {nb_dates}", flush=True)
                    except Exception as e:
                        print(f"       ⚠ Erreur conversion 'MONTH' en date : {e}", flush=True)

                all_dfs.append(df)

        except Exception as e:
            print(f"  [ERROR] {path}: {e}", flush=True)

        sleep(0.05)
        print(f"PROGRESS:{int(idx/total*100)}%", flush=True)

    if not all_dfs:
        sys.exit("Aucune feuille valide trouvée.")

    # ➕ Convertir en majuscules (important)
    devises_detectées = {d.upper() for d in devises_detectées}

    # ✅ Maintenant que les devises sont détectées, on appelle la fonction
    rates = get_ecb_rates(args.date, required_currencies=devises_detectées)
    rates.update(manu)

    zone_affectation_df = None
    table_df = None

    if table_path and os.path.exists(table_path):
        try:
            table_df = pd.read_excel(table_path, sheet_name="table", engine="openpyxl")
            print(f"[INFO] ✅ Table chargé ({table_df.shape[0]} lignes).")
        except Exception as e:
            print(f"[ERROR] ❌ Erreur chargement table : {e}")


    fusion = pd.concat(all_dfs, ignore_index=True)

    print(f"[DEBUG] 📌 Rates récupérés : {rates}", flush=True)
    currencies_in_file = set(fusion["CURRENCY"].dropna().unique())
    print(f"[DEBUG] 📌 Devises trouvées dans les fichiers : {currencies_in_file}", flush=True)
    missing_currencies = currencies_in_file - set(rates.keys())
    if missing_currencies:
        print(f"[ERROR] ❌ Aucune correspondance de taux pour les devises suivantes : {missing_currencies}", flush=True)
        print("         ➡️ Ajoutez-les dans les taux manuels ou vérifiez les données sources.", flush=True)
        sys.exit(1)
    else:
        print("[INFO] ✅ Tous les taux de conversion sont disponibles pour les devises présentes.", flush=True)

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

    else:
        print("[WARN] ❌ Aucune date valide détectée, aucun filtre appliqué.")


    fusion["CURRENCY"] = fusion["CURRENCY"].str.strip().str.upper()
    fusion["Taux €"] = fusion["CURRENCY"].map(rates)

    fusion["C.A en €"] = fusion.apply(
        lambda row: row["TURNOVER"] * row["Taux €"]
        if pd.notnull(row.get("TURNOVER")) and pd.notnull(row.get("Taux €"))
        else None,
        axis=1
    )

    # ➕ Calcul des marges
    fusion["VAR Margin"] = fusion.apply(
        lambda row: row["C.A en €"] - (row["VARIABLE COSTS"] * row["Taux €"] * row["QUANTITY"])
        if pd.notnull(row.get("C.A en €")) and pd.notnull(row.get("VARIABLE COSTS")) and pd.notnull(row.get("Taux €")) and pd.notnull(row.get("QUANTITY"))
        else None,
        axis=1
    )

    fusion["Margin"] = fusion.apply(
        lambda row: row["C.A en €"] - (row["COGS"] * row["Taux €"] * row["QUANTITY"])
        if pd.notnull(row.get("C.A en €")) and pd.notnull(row.get("COGS")) and pd.notnull(row.get("Taux €")) and pd.notnull(row.get("QUANTITY"))
        else None,
        axis=1
    )



    dev_non_gérées = devises_detectées - rates.keys()

    print(f"[INFO] 🏦 Devises détectées dans les fichiers : {sorted(devises_detectées)}", flush=True)
    print(f"[INFO] ✅ Taux disponibles ECB : {sorted(rates.keys())}", flush=True)

    if dev_non_gérées:
        print(f"[WARN] ⚠ Les devises suivantes n'ont pas de taux ECB : {sorted(dev_non_gérées)}", flush=True)
    else:
        print(f"[INFO] 🎉 Tous les taux de devises sont disponibles 🎯", flush=True)


    ORDER = [
    "MONTH", "SIAMP UNIT", "SALE TYPE", "TYPE OF CANAL", "CUSTOMER NAME",
    "COMMERCIAL AREA", "SUR FAMILLE", "FAMILLE", "REFERENCE", "PRODUCT NAME",
    "QUANTITY", "TURNOVER", "CURRENCY", "COUNTRY", "C.A en €",
    "VARIABLE COSTS", "COGS", "VAR Margin", "Margin",
    "NOMFICHIER", "FEUILLE", "Enseigne ret", "Sur famille"
]


    if fusion.empty:
        print("[ERROR] ❌ Aucune donnée après le filtrage, arrêt du script.", flush=True)
        sys.exit(1)

    fusion = fusion[[c for c in ORDER if c in fusion.columns]
                    + [c for c in fusion.columns if c not in ORDER]]
    fusion.to_excel(out, index=False)
    print(f"[DEBUG] 📄 Fichier Excel sauvegardé : {out}", flush=True)
    print(f"[DEBUG] 📏 Shape du DataFrame fusionné : {fusion.shape}", flush=True)

    # mise en forme Excel
    print("[DEBUG] 🟡 Début de la mise en forme Excel...", flush=True)
    try:
        wb = load_workbook(out)
        ws = wb.active

        print(f"[DEBUG] 📊 Workbook chargé : {out}", flush=True)
        print(f"[DEBUG] Nombre de lignes : {ws.max_row}, Nombre de colonnes : {ws.max_column}", flush=True)

        if ws.max_row > 1 and ws.max_column > 0:
            last_col_letter = get_column_letter(ws.max_column)
            last_row = ws.max_row
            table_range = f"A1:{last_col_letter}{last_row}"
            print(f"[DEBUG] 🖋️ Définition de la table FusionTable sur la plage : {table_range}", flush=True)

            table = Table(displayName="FusionTable", ref=table_range)
            table.tableStyleInfo = TableStyleInfo(
                name="TableStyleMedium9",
                showFirstColumn=False,
                showLastColumn=False,
                showRowStripes=True,
                showColumnStripes=False
            )
            
            # ─── Videz d’abord toute table existante ───────────────────────
            ws._tables.clear()

            # ─── Ajout de la nouvelle table ───────────────────────────────
            ws.add_table(table)
            print("[DEBUG] ✅ Nouvelle table 'FusionTable' ajoutée avec succès", flush=True)


            # ➕ Formatage des colonnes €
            EURO_COLUMNS = {"C.A en €", "VAR Margin", "Margin"}
            print("[DEBUG] 🎯 Formatage des colonnes €...", flush=True)
            for col_idx in range(1, ws.max_column + 1):
                header = ws.cell(row=1, column=col_idx).value
                if header in EURO_COLUMNS:
                    for row_idx in range(2, last_row + 1):
                        cell = ws.cell(row=row_idx, column=col_idx)
                        cell.number_format = u"#,##0.00\u00a0€"
            print("[DEBUG] ✅ Formatage des colonnes € terminé", flush=True)
        else:
            print("[WARN] ⚠️ Impossible d'ajouter la table : pas assez de données (0 colonne ou 1 ligne).", flush=True)

        wb.save(out)
        print(f"\n✅ Fusion terminée – fichier créé : {out}\n", flush=True)

    except Exception as e:
        print(f"[ERROR] ❌ Une erreur s'est produite pendant la mise en forme Excel : {e}", flush=True)
        sys.exit(1)


def extract_year_from_filename(filename):
    import re
    match = re.search(r"20\\d{2}", os.path.basename(filename))
    return int(match.group()) if match else None


def run_etl(fichiers: list[str], chemin_sortie: str) -> bool:
    """
    Traite une liste de fichiers Excel, les fusionne, nettoie les dates,
    et exporte le résultat en CSV.
    """
    try:
        all_dfs = []
        for path in fichiers:
            print(f"[INFO] Chargement : {path}", flush=True)
            df = pd.read_excel(path, engine="openpyxl")
            df.columns = df.columns.str.strip().str.upper()

            year = extract_year_from_filename(path)
            df = format_date_column(df, year)

            all_dfs.append(df)

        if not all_dfs:
            print("❌ Aucun fichier valide à fusionner.")
            return False

        fusion = pd.concat(all_dfs, ignore_index=True)

        # Nettoyage éventuel
        fusion.drop(columns=["DATE_TEMP", "DATE_TEMP_NUM"], errors="ignore", inplace=True)

        # Export CSV
        out_csv = chemin_sortie if chemin_sortie.endswith(".csv") else chemin_sortie + ".csv"
        fusion.to_csv(out_csv, index=False, encoding="utf-8")
        print(f"✅ Fichier exporté : {out_csv}")
        return True

    except Exception as e:
        print(f"[ERROR] ❌ Une erreur est survenue dans l'ETL : {e}")
        return False
