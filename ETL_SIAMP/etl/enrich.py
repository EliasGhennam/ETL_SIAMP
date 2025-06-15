# etl/enrich.py
import pandas as pd

def enrich_dataframe(df, zone_affect_path, table_path):
    if "REFERENCE" not in df.columns:
        return df

    try:
        ref_zone = pd.read_excel(zone_affect_path)
        ref_zone.columns = ref_zone.columns.str.strip().str.upper()
        df = df.merge(ref_zone[["REFERENCE", "COMMERCIAL AREA"]], on="REFERENCE", how="left")
    except Exception:
        pass

    try:
        ref_table = pd.read_excel(table_path)
        ref_table.columns = ref_table.columns.str.strip().str.upper()
        df = df.merge(ref_table[["REFERENCE", "ENSEIGNE", "SUR FAMILLE"]], on="REFERENCE", how="left")
    except Exception:
        pass

    return df

    # ---------------------------- ZONE AFFECTATION ----------------------------
    try:
        zone_affectation_df = pd.read_excel(
            zone_affectation_path,
            sheet_name="ZONE AFFECTATION",
            usecols="A,E",  # A = PAYS, E = Zone commerciale
            engine="openpyxl"
        )
        zone_affectation_df.columns = ["PAYS", "COMMERCIAL AREA"]
        fusion["COUNTRY"] = fusion["COUNTRY"].astype(str).str.strip().str.upper()
        zone_affectation_df["PAYS"] = zone_affectation_df["PAYS"].astype(str).str.strip().str.upper()
        
        fusion = fusion.merge(zone_affectation_df, how="left", left_on="COUNTRY", right_on="PAYS")
        fusion.drop(columns=["PAYS"], inplace=True)
        if "COMMERCIAL AREA_x" in fusion.columns and "COMMERCIAL AREA_y" in fusion.columns:
            fusion.drop(columns=["COMMERCIAL AREA_x"], inplace=True)
            fusion.rename(columns={"COMMERCIAL AREA_y": "COMMERCIAL AREA"}, inplace=True)
        elif "COMMERCIAL AREA_y" in fusion.columns:
            fusion.rename(columns={"COMMERCIAL AREA_y": "COMMERCIAL AREA"}, inplace=True)
        print(f"[INFO] ✅ Fusion COMMERCIAL AREA effectuée.")
    except Exception as e:
        print(f"[ERROR] ❌ Erreur fusion ZONE AFFECTATION : {e}")
        traceback.print_exc()

    # ---------------------------- SUR FAMILLE ----------------------------
    try:
        # Nettoyage préalable
        fusion["REFERENCE"] = fusion["REFERENCE"].astype(str).str.strip()
        table_df.iloc[:, 14] = table_df.iloc[:, 14].astype(str).str.strip()  # colonne O

        # Fusion sans écraser l’existante
        fusion = fusion.merge(
            table_df[[table_df.columns[14], table_df.columns[16]]].rename(columns={
                table_df.columns[14]: "REFERENCE",
                table_df.columns[16]: "Sur-famille"  # ⚠️ Respectez bien la casse
            }),
            how="left",
            on="REFERENCE"
        )

        print("[INFO] ✅ Colonne 'Sur famille' fusionnée et 'SUR FAMILLE' consolidée.")
        def nettoyer_cellules(df):
            return df.applymap(
                lambda x: (
                    re.sub(r'[^\x09\x0A\x0D\x20-\x7E\u00A0-\uFFFF]', '', str(x))
                    if isinstance(x, str) else x
                )
            )
        fusion = nettoyer_cellules(fusion)

    except Exception as e:
        print(f"[ERROR] ❌ Erreur fusion SUR FAMILLE : {e}")
        traceback.print_exc()


    # ---------------------------- ENSEIGNE RET ----------------------------
    try:
        fusion["ENSEIGNE"] = fusion["ENSEIGNE"].fillna("").astype(str).str.strip()
        fusion["CUSTOMER NAME"] = fusion["CUSTOMER NAME"].fillna("").astype(str).str.strip()
        fusion["concat_key"] = fusion["ENSEIGNE"] + fusion["CUSTOMER NAME"]

        table_df["concat_key"] = table_df.iloc[:, 21].astype(str).str.strip()  # colonne V dans table

        fusion = fusion.merge(
            table_df[["concat_key", table_df.columns[22]]].rename(columns={table_df.columns[22]: "Enseigne ret"}),  # colonne W
            how="left",
            on="concat_key"
        )
        fusion.drop(columns=["concat_key"], inplace=True)
        print(f"[INFO] ✅ Fusion Enseigne ret effectuée.")
    except Exception as e:
        print(f"[ERROR] ❌ Erreur fusion Enseigne ret : {e}")
        traceback.print_exc()

    # Supprimer la colonne 'ENSEIGNE' car elle n'est pas utile (copie de CUSTOMER NAME)
    if "ENSEIGNE" in fusion.columns:
        fusion.drop(columns=["ENSEIGNE"], inplace=True)
        print(f"[INFO] 🗑️ Colonne 'ENSEIGNE' supprimée (inutile car remplacée par 'Enseigne ret').")
