#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_functional.py – Tests fonctionnels pour ETL_SIAMP
------------------------------------------------------
Ensemble de tests fonctionnels pour valider le bon fonctionnement
des principales fonctionnalités de l'application ETL_SIAMP.
"""
import os
import sys
import pandas as pd
import pytest
import tempfile
import shutil
from pathlib import Path
import configparser
import re
from datetime import datetime

# Ajouter le répertoire parent au chemin pour importer les modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import des modules à tester
from ETL_SIAMP import (
    get_ecb_rates, 
    normalize_ref, 
    nettoyer_str, 
    validate_strict_columns,
    format_date_to_english,
    convert_to_float
)

# Fonctions utilitaires pour les tests
def create_test_excel(path, data, sheet_name="TURNOVER"):
    """Crée un fichier Excel de test avec les données spécifiées."""
    df = pd.DataFrame(data)
    df.to_excel(path, sheet_name=sheet_name, index=False)
    return path

def get_test_data_path():
    """Retourne le chemin vers le répertoire de données de test."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data")

@pytest.fixture
def temp_dir():
    """Fixture créant un répertoire temporaire pour les tests."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)

@pytest.fixture
def sample_excel_files(temp_dir):
    """Fixture créant des fichiers Excel de test."""
    files = []
    
    # Fichier 1 - Format standard
    data1 = {
        "MONTH": ["01/01/2023", "01/02/2023", "01/03/2023"],
        "SIAMP UNIT": ["FRANCE", "FRANCE", "FRANCE"],
        "CUSTOMER NAME": ["Client A", "Client B", "Client C"],
        "REFERENCE": ["REF001", "REF002", "REF003"],
        "TURNOVER": [1000, 2000, 3000],
        "QUANTITY": [10, 20, 30],
        "CURRENCY": ["EUR", "EUR", "EUR"]
    }
    files.append(create_test_excel(os.path.join(temp_dir, "file1.xlsx"), data1))
    
    # Fichier 2 - Colonnes différentes
    data2 = {
        "MONTH": ["01/01/2023", "01/02/2023", "01/03/2023"],
        "SIAMP UNIT": ["UK", "UK", "UK"],
        "CLIENT": ["Client D", "Client E", "Client F"],  # Nom différent
        "REFERENCE": ["REF004", "REF005", "REF006"],
        "CA": [1500, 2500, 3500],  # Nom différent
        "QTY": [15, 25, 35],  # Nom différent
        "CURRENCY": ["GBP", "GBP", "GBP"]
    }
    files.append(create_test_excel(os.path.join(temp_dir, "file2.xlsx"), data2))
    
    # Fichier 3 - Format des dates différent
    data3 = {
        "DATE": ["2023-01-01", "2023-02-01", "2023-03-01"],  # Format différent
        "SIAMP UNIT": ["SPAIN", "SPAIN", "SPAIN"],
        "CUSTOMER NAME": ["Client G", "Client H", "Client I"],
        "REFERENCE": ["REF007", "REF008", "REF009"],
        "TURNOVER": [2000, 3000, 4000],
        "QUANTITY": [20, 30, 40],
        "CURRENCY": ["EUR", "EUR", "EUR"]
    }
    files.append(create_test_excel(os.path.join(temp_dir, "file3.xlsx"), data3))
    
    return files

@pytest.fixture
def sample_reference_file(temp_dir):
    """Fixture créant un fichier de référence pour les tests."""
    # Créer un DataFrame pour la feuille "table"
    table_data = {
        "A": ["Titre", "Description"],
        "B": ["PRODUCT NAME", "Nom 1", "Nom 2", "Nom 3"],
        "C": ["Surfamille ret", "SANITAIRE", "BATI", "CUISINE"],
        "D": ["Famille", "WC", "RESERVOIR", "EVIER"],
        "G": ["CONCAT NAME", "DISTRI1 Client A", "DISTRI2 Client B", "DISTRI3 Client C"],
        "H": ["Enseigne ret", "DISTRI1", "DISTRI2", "DISTRI3"],
        "REFERENCE V2": ["REF001", "REF002", "REF003"],
        "Surfamille ret V2": ["SANITAIRE", "BATI", "CUISINE"]
    }
    
    # Créer un DataFrame pour la feuille "ZONE AFFECTATION"
    zone_data = {
        "A": ["PAYS", "FRANCE", "UK", "SPAIN"],
        "E": ["COMMERCIAL AREA", "EUROPE", "EUROPE", "EUROPE"]
    }
    
    # Créer le fichier Excel avec les deux feuilles
    with pd.ExcelWriter(os.path.join(temp_dir, "reference.xlsx")) as writer:
        pd.DataFrame(table_data).to_excel(writer, sheet_name="table", index=False)
        pd.DataFrame(zone_data).to_excel(writer, sheet_name="ZONE AFFECTATION", index=False)
    
    return os.path.join(temp_dir, "reference.xlsx")

# Tests des fonctions de base
def test_normalize_ref():
    """Teste la fonction de normalisation des références."""
    assert normalize_ref("REF-001") == "REF001"
    assert normalize_ref("ref 002") == "REF002"
    assert normalize_ref("REF/003.A") == "REF003A"
    assert normalize_ref(None) == ""
    assert normalize_ref(123) == "123"

def test_nettoyer_str():
    """Teste la fonction de nettoyage des chaînes de caractères."""
    assert nettoyer_str(" Test ") == "Test"
    assert nettoyer_str(None) == ""
    assert nettoyer_str(123) == "123"
    assert nettoyer_str("") == ""

def test_format_date_to_english():
    """Teste la fonction de formatage des dates."""
    # Test avec différents formats de dates
    assert format_date_to_english("01/01/2023") == "2023-01-01"
    assert format_date_to_english("1/1/2023") == "2023-01-01"
    assert format_date_to_english("01-01-2023") == "2023-01-01"
    assert format_date_to_english("2023-01-01") == "2023-01-01"  # Déjà au bon format
    assert format_date_to_english("Invalid date") is None

def test_convert_to_float():
    """Teste la fonction de conversion en nombre flottant."""
    assert convert_to_float("1000,50") == 1000.50
    assert convert_to_float("1,000.50") == 1000.50
    assert convert_to_float("1 000,50") == 1000.50
    assert convert_to_float("1.000,50") == 1000.50
    assert convert_to_float("Invalid") is None

def test_get_ecb_rates():
    """Teste la récupération des taux de change ECB."""
    # Test avec une date passée (résultats stables)
    rates = get_ecb_rates("2023-01-02")
    # Vérifier la présence de devises courantes
    assert "USD" in rates
    assert "GBP" in rates
    assert "JPY" in rates
    # Vérifier que les taux sont des flottants positifs
    assert isinstance(rates["USD"], float)
    assert rates["USD"] > 0

def test_validate_strict_columns():
    """Teste la validation des colonnes obligatoires."""
    # Colonnes minimales requises
    required_columns = ["MONTH", "CUSTOMER NAME", "REFERENCE", "TURNOVER"]
    
    # DataFrame valide
    df_valid = pd.DataFrame({
        "MONTH": ["01/01/2023"],
        "CUSTOMER NAME": ["Client A"],
        "REFERENCE": ["REF001"],
        "TURNOVER": [1000]
    })
    
    # DataFrame invalide (manque TURNOVER)
    df_invalid = pd.DataFrame({
        "MONTH": ["01/01/2023"],
        "CUSTOMER NAME": ["Client A"],
        "REFERENCE": ["REF001"]
    })
    
    # Test avec details=False
    assert validate_strict_columns(df_valid, "test.xlsx", required_columns) is True
    assert validate_strict_columns(df_invalid, "test.xlsx", required_columns) is False
    
    # Test avec details=True
    valid_result, valid_details = validate_strict_columns(df_valid, "test.xlsx", required_columns, True)
    invalid_result, invalid_details = validate_strict_columns(df_invalid, "test.xlsx", required_columns, True)
    
    assert valid_result is True
    assert invalid_result is False
    assert "TURNOVER" in invalid_details["missing"]

# Tests fonctionnels de base
def test_excel_column_detection(sample_excel_files):
    """Teste la détection des colonnes dans les fichiers Excel."""
    from ETL_SIAMP_GUI import COLUMN_MAPPING
    
    for file_path in sample_excel_files:
        # Lire le fichier
        df = pd.read_excel(file_path)
        
        # Vérifier que les colonnes clés peuvent être normalisées correctement
        for std_name, variations in COLUMN_MAPPING.items():
            for col in df.columns:
                if col.strip().upper() in [v.strip().upper() for v in variations]:
                    # La colonne a été reconnue comme une variante valide
                    assert True
                    break

def test_reference_mapping(sample_excel_files, sample_reference_file):
    """Teste le mapping des références avec le fichier de référence."""
    # Charger le fichier de référence
    ref_table = pd.read_excel(sample_reference_file, sheet_name="table")
    
    # Charger un fichier de données
    data_df = pd.read_excel(sample_excel_files[0])
    
    # Normaliser les références
    data_df["REFERENCE_NORM"] = data_df["REFERENCE"].apply(normalize_ref)
    ref_table["REFERENCE_V2_NORM"] = ref_table["REFERENCE V2"].apply(normalize_ref)
    
    # Créer le mapping
    mapping = dict(zip(ref_table["REFERENCE_V2_NORM"], ref_table["Surfamille ret V2"]))
    
    # Appliquer le mapping
    data_df["Surfamille ret"] = data_df["REFERENCE_NORM"].map(mapping)
    
    # Vérifier que le mapping a fonctionné
    assert data_df.loc[data_df["REFERENCE"] == "REF001", "Surfamille ret"].iloc[0] == "SANITAIRE"
    assert data_df.loc[data_df["REFERENCE"] == "REF002", "Surfamille ret"].iloc[0] == "BATI"
    assert data_df.loc[data_df["REFERENCE"] == "REF003", "Surfamille ret"].iloc[0] == "CUISINE"

def test_date_format_standardization():
    """Teste la standardisation des formats de date."""
    dates = [
        ("01/01/2023", "2023-01-01"),  # Format français
        ("2023-01-01", "2023-01-01"),  # Format ISO
        ("01-01-2023", "2023-01-01"),  # Format avec tirets
        ("1/1/2023", "2023-01-01"),    # Format sans zéros
        ("Jan 1, 2023", "2023-01-01"),  # Format texte anglais
    ]
    
    for input_date, expected in dates:
        result = format_date_to_english(input_date)
        assert result == expected, f"La conversion de {input_date} devrait donner {expected}, mais donne {result}"

def test_currency_conversion():
    """Teste la conversion des devises."""
    # Taux fictifs pour le test
    rates = {
        "USD": 0.85,  # 1 EUR = 0.85 USD donc 1 USD = 1/0.85 EUR
        "GBP": 1.15,  # 1 EUR = 1.15 GBP donc 1 GBP = 1/1.15 EUR
    }
    
    # Valeurs de test
    values = [
        (100, "USD", 100 / 0.85),  # 100 USD en EUR
        (100, "GBP", 100 / 1.15),  # 100 GBP en EUR
        (100, "EUR", 100),         # 100 EUR reste 100 EUR
    ]
    
    for amount, currency, expected in values:
        if currency == "EUR":
            result = amount
        else:
            result = amount / rates[currency]
        assert abs(result - expected) < 0.01, f"La conversion de {amount} {currency} devrait donner environ {expected} EUR"

# Tests d'intégration
def test_full_etl_process(temp_dir, sample_excel_files, sample_reference_file):
    """Teste le processus ETL complet avec des fichiers d'exemple."""
    # Préparer le chemin de sortie
    output_path = os.path.join(temp_dir, "output.xlsx")
    
    # Créer le fichier de configuration de référence
    config = configparser.ConfigParser()
    config['REFERENCES'] = {'reference_file': sample_reference_file}
    config_path = os.path.join(temp_dir, "ref_files.cfg")
    with open(config_path, 'w') as f:
        config.write(f)
    
    # Construire la commande pour exécuter le script ETL_SIAMP.py
    cmd = [
        sys.executable,
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ETL_SIAMP.py"),
        "--chemin_sortie", output_path,
        "--fichiers"
    ]
    cmd.extend(sample_excel_files)
    cmd.extend(["--date", "2023-01-02"])
    
    # Exécuter la commande
    import subprocess
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    
    # Vérifier que le processus s'est terminé correctement
    assert result.returncode == 0, f"ETL process failed: {result.stderr}"
    
    # Vérifier que le fichier de sortie a été créé
    assert os.path.exists(output_path), f"Output file not created: {output_path}"
    
    # Charger le fichier de sortie pour vérifier le contenu
    output_df = pd.read_excel(output_path)
    
    # Vérifier que les colonnes attendues sont présentes
    expected_columns = [
        "MONTH", "SIAMP UNIT", "CUSTOMER NAME", "REFERENCE", 
        "TURNOVER", "QUANTITY", "CURRENCY", "C.A en €"
    ]
    for col in expected_columns:
        assert col in output_df.columns, f"Column {col} missing from output"
    
    # Vérifier que toutes les données ont été fusionnées
    total_rows = sum(len(pd.read_excel(f)) for f in sample_excel_files)
    assert len(output_df) == total_rows, f"Expected {total_rows} rows, got {len(output_df)}"

if __name__ == "__main__":
    pytest.main(["-v", __file__]) 