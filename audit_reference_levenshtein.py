import pandas as pd
from rapidfuzz import process, fuzz

# --- Paramètres ---
ret_path = r"fichiers_excel/Traitement Mensuel/V4/RET 2.xlsx"
sheet_name = "table"
not_found_csv = "references_non_trouvees.csv"
output_csv = "references_non_trouvees_levenshtein.csv"

# --- Charger la table de référence ---
ret = pd.read_excel(ret_path, sheet_name=sheet_name, dtype=str)
ret['REFERENCE V2'] = ret['REFERENCE V2'].astype(str).str.strip().str.upper()

# --- Charger les références non trouvées ---
not_found = pd.read_csv(not_found_csv, dtype=str)
not_found['REFERENCE'] = not_found['REFERENCE'].astype(str).str.strip().str.upper()

# --- Pour chaque référence non trouvée, trouver les 5 plus proches dans REFERENCE V2 ---
results = []
ref_v2_list = ret['REFERENCE V2'].dropna().unique().tolist()
for ref in not_found['REFERENCE']:
    matches = process.extract(ref, ref_v2_list, scorer=fuzz.ratio, limit=5)
    for i, (match, score, _) in enumerate(matches, 1):
        results.append({
            'REFERENCE_NON_TROUVEE': ref,
            f'MATCH_{i}': match,
            f'SCORE_{i}': score
        })

# --- Réorganiser les résultats pour avoir une ligne par référence non trouvée ---
from collections import defaultdict
agg = defaultdict(dict)
for row in results:
    ref = row['REFERENCE_NON_TROUVEE']
    for k, v in row.items():
        if k != 'REFERENCE_NON_TROUVEE':
            agg[ref][k] = v

data = []
for ref in not_found['REFERENCE']:
    row = {'REFERENCE_NON_TROUVEE': ref}
    row.update(agg.get(ref, {}))
    data.append(row)

pd.DataFrame(data).to_csv(output_csv, index=False, encoding='utf-8')
print(f"Export terminé : {output_csv}") 