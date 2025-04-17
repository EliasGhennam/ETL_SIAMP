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
def get_conversion_rates(date: str | None = None) -> dict[str, float]:
    """
    Récupère un dict de taux de change en base EUR.
    - date=None          : temps réel via /rates
    - date="YYYY-MM-DD"  : historique via /historical
    En cas de 400 sur l'historique (plan gratuit), bascule sur le temps réel.
    """
    if date:
        url    = "https://currencyapi.net/api/v1/historical"
        params = {"key": API_KEY, "date": date, "base_currency": "EUR"}
        ctx    = f" au {date}"
    else:
        url    = "https://currencyapi.net/api/v1/rates"
        params = {"key": API_KEY, "base_currency": "EUR"}
        ctx    = " en temps réel"

    try:
        resp = requests.get(url, params=params, timeout=10)
        # si historique non dispo (400), retomber sur rates
        if resp.status_code == 400 and date:
            print(f"[WARN] Historique non dispo ({resp.status_code}), bascule en temps réel.")
            return get_conversion_rates(date=None)

        resp.raise_for_status()
        data = resp.json()
        if not data.get("valid", False):
            raise ValueError("Réponse API invalide")

        raw = data.get("rates", {})
        rates: dict[str, float] = {}
        for code, val in raw.items():
            try:
                f = float(val)
                if f != 0:
                    rates[code.upper()] = f
            except Exception:
                continue
        # garantissons qu'EUR existe à 1
        rates["EUR"] = 1.0

        print(f"[INFO] ✅ Taux de conversion{ctx} chargés :")
        for k, v in rates.items():
            print(f"  → {k} = {round(v,6)} (unités/{ '€' if date is None else '€' })")
        return rates

    except Exception as e:
        print(f"[ERROR] Impossible de charger les taux{ctx} : {e}")
        print("[INFO] Repli sur taux locaux par défaut…")
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
