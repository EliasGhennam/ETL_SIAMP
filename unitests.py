import subprocess
import pandas as pd
from pathlib import Path

def test_fusion_etl(tmp_path):
    # Chemin vers un fichier Excel de test (à créer dans tests/)
    test_xlsx = "tests/input1.xlsx"
    output_file = tmp_path / "fusion.xlsx"

    # Lancement du script avec les bons arguments
    subprocess.run([
        "python", "ETL_SIAMP.py",
        "--fichiers", test_xlsx,
        "--chemin_sortie", str(output_file),
        "--taux_manuels", "USD=0.93",
        "--date", "2024-01-01",
        "--mois_selectionnes", "2025-01,2025-02,2025-03"
    ], check=True)


    # Vérifie que le fichier est bien généré
    assert output_file.exists(), "❌ Le fichier de sortie n'a pas été créé."

    # Vérifie qu'il contient des données exploitables
    df = pd.read_excel(output_file)
    assert not df.empty, "❌ Le fichier est vide."
    assert "C.A en €" in df.columns, "❌ La colonne 'C.A en €' est absente."

    print("✅ Test de fusion ETL réussi.")

if __name__ == "__main__":
    from tempfile import TemporaryDirectory
    with TemporaryDirectory() as tmp:
        test_fusion_etl(Path(tmp))
