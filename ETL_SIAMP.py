#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ETL_SIAMP.py – fusion & enrichissement Turnover
-------------------------------------------------
• Ajoute VARIABLE COSTS (CD+FSD) et COGS (PRU) quelle que soit l’écriture.
• Maintient le calcul « C.A en € ».
• Prend en charge un argument --date pour taux historiques via currencyapi.net.
• Replace les colonnes dans l’ordre métier.
"""
from __future__ import annotations
import argparse, glob, io, os, re, sys, warnings
from time import sleep

import pandas as pd
import requests
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.utils import get_column_letter

# ------------------------------------------------------------------ console UTF‑8
if sys.stdout and hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

***REMOVED***

# ------------------------------------------------------------------ taux de change
def get_live_conversion_rates(date: str | None = None) -> dict[str, float]:
    """
    Récupère les taux via currencyapi.net.
    Si `date` est fournie (YYYY-MM-DD), récupère les taux historiques.
    """
    url = "https://currencyapi.net/api/v1/rates"
    params = {"key": API_KEY}
    if date:
        params["date"] = date
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data.get("valid", False):
            raise ValueError("clé API invalide ou réponse incorrecte")
        rates = {k.upper(): float(v) for k, v in data["rates"].items() if float(v)}
        rates["EUR"] = 1.0
        print("[INFO] ✅ taux API chargés" + (f" ({date})" if date else ""))
        return rates
    except Exception as e:
        print(f"[WARN] API devise indisponible : {e} – repli local")
        return {
            "EUR":1.0,"USD":0.93,"GBP":1.15,"EGP":0.03,
            "CHF":1.04,"AED":0.25,"JPY":0.0062
        }

# ------------------------------------------------------------------ CLI
def main():
    p = argparse.ArgumentParser(description="Fusion fichiers Turnover")
    p.add_argument("--fichiers", nargs='+', required=True)
    p.add_argument("--chemin_sortie", required=True)
    p.add_argument("--taux_manuels")
    p.add_argument("--date", help="Date pour taux historiques (YYYY-MM-DD)")
    args = p.parse_args()

    # parse taux manuels
    def parse_taux_manuels(s: str | None) -> dict[str, float]:
        d = {}
        if not s: return d
        for part in s.split(","):
            try:
                c, v = part.split("=")
                d[c.strip().upper()] = float(v)
            except:
                print(f"[WARN] taux manuel ignoré : {part}")
        return d

    # collecte fichiers
    files: list[str] = []
    for patt in args.fichiers:
        files.extend(glob.glob(patt))
    files = [f for f in files if f.lower().endswith(".xlsx") and not os.path.basename(f).startswith("~$")]
    if not files:
        sys.exit("Aucun fichier .xlsx trouvé.")

    # prépare sortie
    out = args.chemin_sortie
    if not out.lower().endswith(".xlsx"):
        out += ".xlsx"
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)

    # patterns
    TURN_SHEET = re.compile(r"^TURNOVER($|\s+\w+\s+\d+)$", re.I)
    VAR_PATTS  = [r"^CD\s*\+\s*FSD", r"^VARIABLE\s*COSTS?"]
    COGS_PATTS = [r"^PRU", r"^COGS"]

    # récupère taux (avec historical si demandé)
    rates = get_live_conversion_rates(args.date)
    # sauce manuelle
    manual = parse_taux_manuels(args.taux_manuels)
    rates.update(manual)

    all_df: list[pd.DataFrame] = []
    for idx, path in enumerate(files, 1):
        print(f"[{idx}/{len(files)}] {os.path.basename(path)}")
        try:
            xls = pd.ExcelFile(path, engine="openpyxl")
            for sh in filter(TURN_SHEET.match, xls.sheet_names):
                df = xls.parse(sh, usecols="A:Q")
                df.dropna(axis=1, how="all", inplace=True)
                df.columns = [c.strip().upper() for c in df.columns]

                # renommage
                ren = {}
                for c in df.columns:
                    u = c.upper()
                    if any(re.match(p, u) for p in VAR_PATTS):
                        ren[c] = "VARIABLE COSTS"
                    elif any(re.match(p, u) for p in COGS_PATTS):
                        ren[c] = "COGS"
                    elif u == "TURNOVER":
                        ren[c] = "TURNOVER"
                    elif u == "CURRENCY":
                        ren[c] = "CURRENCY"
                    elif u in ("CUSTOMER","CUSTOMER NAME"):
                        ren[c] = "CUSTOMER NAME"
                df.rename(columns=ren, inplace=True)

                # log detection
                has_var = "VARIABLE COSTS" in df.columns
                has_cogs= "COGS" in df.columns
                if not (has_var or has_cogs):
                    print(f"  [INFO] ni VARIABLE COSTS ni COGS dans « {sh} »")
                else:
                    if has_var:
                        nv = df["VARIABLE COSTS"].notna().sum()
                        print(f"   • VARIABLE COSTS → {nv} valeurs non nulles")
                    if has_cogs:
                        nc = df["COGS"].notna().sum()
                        print(f"   • COGS           → {nc} valeurs non nulles")

                # calcul C.A en €
                if {"CURRENCY","TURNOVER"} <= set(df.columns):
                    df.insert(
                        df.columns.get_loc("TURNOVER")+1,
                        "C.A en €",
                        df.apply(lambda r: round(r["TURNOVER"] / rates.get(r["CURRENCY"],1),2)
                                 if pd.notna(r["TURNOVER"]) else None, axis=1)
                    )

                df["NOMFICHIER"] = os.path.basename(path)
                df["FEUILLE"]     = sh
                all_df.append(df)
        except Exception as e:
            print(f"  [ERR] {path}: {e}")
        sleep(0.05)
        print(f"PROGRESS:{int(idx/len(files)*100)}%")

    if not all_df:
        sys.exit("Aucune feuille valide trouvée.")

    # fusion & ordre
    fusion = pd.concat(all_df, ignore_index=True)
    ORDER = [
        "MONTH","SIAMP UNIT","SALE TYPE","TYPE OF CANAL","ENSEIGNE","CUSTOMER NAME",
        "COMMERCIAL AREA","SUR FAMILLE","FAMILLE","REFERENCE","PRODUCT NAME",
        "QUANTITY","TURNOVER","CURRENCY","COUNTRY","C.A en €",
        "VARIABLE COSTS","COGS","NOMFICHIER","FEUILLE"
    ]
    cols = [c for c in ORDER if c in fusion.columns] + [c for c in fusion.columns if c not in ORDER]
    fusion = fusion[cols]
    fusion.to_excel(out, index=False)

    # mise en forme Excel
    wb = load_workbook(out)
    ws = wb.active
    ws.add_table(Table(
        displayName="FusionTable",
        ref=f"A1:{get_column_letter(ws.max_column)}{ws.max_row}",
        tableStyleInfo=TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
    ))
    for col in ws.iter_cols(min_row=2, max_col=ws.max_column):
        if ws[f"{col[0].column_letter}1"].value == "C.A en €":
            for cell in col:
                cell.number_format = u"#,##0.00\u00a0€"
    wb.save(out)

    print(f"\n✅ Fusion terminée – fichier créé : {out}\n")


if __name__ == "__main__":
    main()
