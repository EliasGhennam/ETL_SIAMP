import pandas as pd
import glob
import os

# --- Paramètres ---
ret_path = r"fichiers_excel/Traitement Mensuel/V4/RET 2.xlsx"
sheet_name = "table"
origin_dir = r"fichiers_excel/Traitement Mensuel/V4"
output_csv = "references_non_trouvees.csv"

# --- Charger la table de référence ---
ret = pd.read_excel(ret_path, sheet_name=sheet_name, dtype=str)
ret['REFERENCE V2'] = ret['REFERENCE V2'].astype(str).str.strip().str.upper()
ret['REFERENCE'] = ret['REFERENCE'].astype(str).str.strip().str.upper()

# --- Extraire toutes les références uniques des fichiers d'origine ---
all_refs = set()
for file in glob.glob(os.path.join(origin_dir, '*.xlsx')):
    if os.path.basename(file).lower().startswith('ret'):
        continue  # ignorer le fichier RET
    try:
        xls = pd.ExcelFile(file)
        for sh in xls.sheet_names:
            try:
                df = pd.read_excel(file, sheet_name=sh, dtype=str)
                if 'REFERENCE' in df.columns:
                    refs = df['REFERENCE'].astype(str).str.strip().str.upper().unique()
                    all_refs.update(refs)
            except Exception as e:
                print(f"[WARN] Feuille ignorée : {file} / {sh} – {e}")
    except Exception as e:
        print(f"[WARN] Fichier ignoré : {file} – {e}")

print(f"Total références uniques trouvées dans les fichiers d'origine : {len(all_refs)}")

# --- Vérifier la présence dans le RET ---
ret_v2_set = set(ret['REFERENCE V2'])
ret_old_set = set(ret['REFERENCE'])

results = []
for ref in sorted(all_refs):
    in_v2 = ref in ret_v2_set
    in_old = ref in ret_old_set
    results.append({
        'REFERENCE': ref,
        'PRESENT DANS REFERENCE V2': in_v2,
        'PRESENT DANS REFERENCE': in_old
    })

# --- Afficher un résumé ---
nb_in_v2 = sum(r['PRESENT DANS REFERENCE V2'] for r in results)
nb_in_old = sum(r['PRESENT DANS REFERENCE'] for r in results)
nb_not_found = sum((not r['PRESENT DANS REFERENCE V2']) and (not r['PRESENT DANS REFERENCE']) for r in results)
print(f"Références trouvées dans REFERENCE V2 : {nb_in_v2}")
print(f"Références trouvées dans REFERENCE (fallback) : {nb_in_old}")
print(f"Références non trouvées du tout : {nb_not_found}")

# --- Exporter les non trouvées ---
not_found = [r for r in results if not r['PRESENT DANS REFERENCE V2'] and not r['PRESENT DANS REFERENCE']]
if not_found:
    pd.DataFrame(not_found).to_csv(output_csv, index=False, encoding='utf-8')
    print(f"Liste des références non trouvées exportée dans {output_csv}")
else:
    print("Toutes les références sont couvertes par le RET !") 