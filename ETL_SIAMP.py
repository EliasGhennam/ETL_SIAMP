#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ETL_SIAMP.py – fusion & enrichissement Turnover

• Récupère les taux historiques si votre plan le permet (/historical),
  sinon bascule automatiquement sur le temps réel (/rates).
• Ajoute VARIABLE COSTS (CD+FSD) et COGS (PRU) quelle que soit l’écriture.
• Maintient le calcul « C.A en € ».
• Réordonne les colonnes métier.
"""
from __future__ import annotations
import argparse
import glob
import io
import os
import re
import sys
import warnings
from time import sleep
from typing import Any
import xml.etree.ElementTree as ET
from datetime import datetime
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
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

def get_ecb_rates():
    url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
    try:
        response = requests.get(url)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        ns = {'ns': 'http://www.ecb.int/vocabulary/2002-08-01/eurofxref'}
        
        # cherche la section Cube avec l'attribut time
        cubes = root.findall(".//Cube[@time]")
        if not cubes:
            raise ValueError("Pas de données de taux trouvées")

        latest_cube = cubes[0]
        date = latest_cube.attrib["time"]
        rates = {"EUR": 1.0}

        for cube in latest_cube.findall("Cube"):
            currency = cube.attrib["currency"]
            rate = float(cube.attrib["rate"])
            rates[currency] = rate

        print(f"[INFO] Taux ECB récupérés au {date}")
        for k, v in rates.items():
            print(f"  → {k} = {v}")
        return rates

    except Exception as e:
        print(f"[ERROR] Erreur récupération ECB : {e}")
        return {
        "EUR":1.0, "USD":0.93, "GBP":1.15,
        "EGP":0.03, "CHF":1.04, "AED":0.25, "JPY":0.0062
    }

# ------------------------------------------------------------------ CLI
def main():
    parser = argparse.ArgumentParser(description="Fusionnez plusieurs fichiers Excel Turnover")
    parser.add_argument("--fichiers",      nargs='+', required=True)
    parser.add_argument("--chemin_sortie", required=True)
    parser.add_argument("--taux_manuels",  help="USD=0.93,GBP=1.15", default=None)
    parser.add_argument("--date",          help="YYYY-MM-DD pour historique (premium)", default=None)
    args = parser.parse_args()

    # parse manuels
    manu: dict[str,float] = {}
    if args.taux_manuels:
        for part in args.taux_manuels.split(","):
            try:
                c,v = part.split("=")
                manu[c.strip().upper()] = float(v)
            except:
                print(f"[WARN] taux manuel ignoré: {part}")

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

    # récupère les taux (historique ou réel)
    rates = get_conversion_rates(args.date)
    rates.update(manu)

    all_dfs: list[pd.DataFrame] = []
    total = len(files)
    for idx, path in enumerate(files, 1):
        print(f"[{idx}/{total}] {os.path.basename(path)}")
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

                print("    -> Colonnes:", ", ".join(df.columns))

                # log var/cogs
                for nm in ("VARIABLE COSTS","COGS"):
                    if nm in df.columns:
                        n = df[nm].notna().sum()
                        print(f"       • {nm} détectée: {n} valeurs non-null")

                # calcul C.A en €
                if {"TURNOVER","CURRENCY"} <= set(df.columns):
                    df.insert(
                        df.columns.get_loc("TURNOVER")+1,
                        "C.A en €",
                        df.apply(lambda r: round(r["TURNOVER"] / rates.get(r["CURRENCY"].upper(),1),2)
                                 if pd.notna(r["TURNOVER"]) else None,
                                 axis=1)
                    )

                df["NOMFICHIER"] = os.path.basename(path)
                df["FEUILLE"]     = sh
                all_dfs.append(df)

        except Exception as e:
            print(f"  [ERROR] {path}: {e}")

        sleep(0.05)
        print(f"PROGRESS:{int(idx/total*100)}%")

    if not all_dfs:
        sys.exit("Aucune feuille valide trouvée.")

    fusion = pd.concat(all_dfs, ignore_index=True)

    ORDER = [
        "MONTH","SIAMP UNIT","SALE TYPE","TYPE OF CANAL","ENSEIGNE","CUSTOMER NAME",
        "COMMERCIAL AREA","SUR FAMILLE","FAMILLE","REFERENCE","PRODUCT NAME",
        "QUANTITY","TURNOVER","CURRENCY","COUNTRY","C.A en €",
        "VARIABLE COSTS","COGS","NOMFICHIER","FEUILLE"
    ]
    fusion = fusion[[c for c in ORDER if c in fusion.columns]
                    + [c for c in fusion.columns if c not in ORDER]]

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
