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

def get_ecb_rates(date: str | None = None, required_currencies: set[str] | None = None):
    print(f"[DEBUG] Appel get_ecb_rates(date={date})", flush=True)
    if date:
        url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"
    else:
        url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"

    try:
        response = requests.get(url)
        print(f"[INFO] 📡 Requête vers {url}", flush=True)
        print(f"[INFO] ✅ Statut : {response.status_code}", flush=True)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        ns = {'ns': 'http://www.ecb.int/vocabulary/2002-08-01/eurofxref'}

        rates = {"EUR": 1.0}
        from datetime import datetime, timedelta

        if date:
            limit_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=60)).strftime("%Y-%m-%d")
            print(f"[INFO] 🔍 Recherche limitée aux taux entre {limit_date} et {date}", flush=True)



        dates = [cube.attrib["time"] for cube in root.findall(".//ns:Cube[@time]", ns)]
        if date:
            dates = sorted([d for d in dates if limit_date <= d <= date], reverse=True)
        else:
            dates = sorted(dates, reverse=True)


        rates_found = set(rates.keys())
        target_cube = None

        for d in dates:
            cube_d = root.find(f".//ns:Cube[@time='{d}']", ns)
            if cube_d is None:
                continue

            for cube in cube_d.findall("ns:Cube", ns):
                cur = cube.attrib["currency"]
                if cur not in rates:
                    rate = float(cube.attrib["rate"])
                    rates[cur] = rate
                    print(f"[INFO] ➕ Taux récupéré pour {cur} au {d} = {rate}", flush=True)
                    rates_found.add(cur)

            if required_currencies and required_currencies <= rates_found:
                print(f"[INFO] ✅ Tous les taux requis trouvés avant {d}", flush=True)
                break


        if date:
            # chercher le jour exact OU le plus proche avant
            dates = [cube.attrib["time"] for cube in root.findall(".//ns:Cube[@time]", ns)]
            print(f"[INFO] 📅 {len(dates)} dates trouvées dans l'historique ECB", flush=True)
            print(f"[INFO] 📅 Premières dates disponibles : {dates[:5]}", flush=True)
            dates.sort(reverse=True)
            target_date = None
            for d in dates:
                if d <= date:
                    target_date = d
                    break

            if not target_date:
                raise ValueError(f"Aucun taux trouvé avant la date {date}")

            target_cube = root.find(f".//ns:Cube[@time='{target_date}']", ns)
            if target_date != date:
                print(f"[INFO] ⚠️ Pas de taux pour {date}, utilisation de {target_date} à la place", flush=True)
            else:
                print(f"[INFO] ✅ Taux trouvés pour la date exacte : {target_date}", flush=True)


            if target_date != date:
                print(f"[INFO] ⚠ Aucun taux pour {date}, substitution par {target_date}", flush=True)
        else:
            # date non spécifiée : dernier taux connu
            cubes = root.findall(".//Cube[@time]")
            if not cubes:
                raise ValueError("Pas de données de taux trouvées")
            target_cube = cubes[0]
            target_date = target_cube.attrib["time"]
        
        print("[INFO] 🔎 Récupération des taux de conversion :", flush=True)
        for cube in target_cube.findall("ns:Cube", ns):
            currency = cube.attrib["currency"]
            raw_rate = float(cube.attrib["rate"])
            print(f"  → {currency} = {raw_rate}", flush=True)
            if raw_rate != 0:
                rates[currency] = raw_rate
        rates["EUR"] = 1.0

        if required_currencies:
            missing = required_currencies - rates_found
            if missing:
                print(f"[WARN] ❌ Aucun taux trouvé pour {sorted(missing)} dans les 60 derniers jours.", flush=True)
                print(f"[SUGGESTION] ✍️ Veuillez les ajouter manuellement dans l'interface ou en ligne de commande.", flush=True)



        print(f"[INFO] Taux ECB récupérés au {date}", flush=True)
        for k, v in rates.items():
            print(f"  → {k} = {v}")
        return rates

    except Exception as e:
        print(f"[ERROR] Erreur récupération ECB : {e}", flush=True)
        print("[FALLBACK] 🛑 Repli sur taux locaux codés en dur", flush=True)
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

    fusion = pd.concat(all_dfs, ignore_index=True)

    fusion["CURRENCY"] = fusion["CURRENCY"].str.strip().str.upper()
    fusion["Taux €"] = fusion["CURRENCY"].map(rates)

    fusion["C.A en €"] = fusion.apply(
        lambda row: row["TURNOVER"] * row["Taux €"]
        if pd.notnull(row.get("TURNOVER")) and pd.notnull(row.get("Taux €"))
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

    print(f"\n✅ Fusion terminée – fichier créé : {out}\n", flush=True)


if __name__ == "__main__":
    main()
