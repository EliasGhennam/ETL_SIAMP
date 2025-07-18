#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_column_mapping.py – Tests pour le mapping des colonnes
----------------------------------------------------------
Tests unitaires pour valider le fonctionnement correct du mapping
des colonnes dans l'application ETL_SIAMP.
"""
import os
import sys
import pandas as pd
import pytest
import tempfile

# Ajouter le répertoire parent au chemin pour importer les modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import des constantes et fonctions
from ETL_SIAMP_GUI import COLUMN_MAPPING, normalize_columns, clean_and_validate_data

@pytest.fixture
def sample_dataframes():
    """Fixture créant des DataFrames de test avec différents formats de colonnes."""
    # DataFrame avec format standard
    df1 = pd.DataFrame({
        "MONTH": ["01/01/2023", "01/02/2023"],
        "CUSTOMER NAME": ["Client A", "Client B"],
        "REFERENCE": ["REF001", "REF002"],
        "TURNOVER": [1000, 2000],
        "QUANTITY": [10, 20],
        "CURRENCY": ["EUR", "USD"]
    })
    
    # DataFrame avec format alternatif
    df2 = pd.DataFrame({
        "DATE": ["01/01/2023", "01/02/2023"],
        "CLIENT": ["Client C", "Client D"],
        "REF": ["REF003", "REF004"],
        "CA": [3000, 4000],
        "QTY": [30, 40],
        "DEVISE": ["GBP", "EUR"]
    })
    
    # DataFrame avec format mixte
    df3 = pd.DataFrame({
        "PERIODE": ["01/01/2023", "01/02/2023"],
        "NOM CLIENT": ["Client E", "Client F"],
        "REFERENCE PRODUIT": ["REF005", "REF006"],
        "CHIFFRE D'AFFAIRE": [5000, 6000],
        "QUANTITE": [50, 60],
        "MONNAIE": ["USD", "GBP"]
    })
    
    return [df1, df2, df3]

def test_column_mapping_detection():
    """Teste la détection des colonnes dans le mapping."""
    # Vérifier que toutes les variantes sont correctement définies
    month_variants = COLUMN_MAPPING["MONTH"]
    assert "MONTH" in month_variants
    assert "DATE" in month_variants
    assert "PERIODE" in month_variants
    
    customer_variants = COLUMN_MAPPING["CUSTOMER NAME"]
    assert "CUSTOMER NAME" in customer_variants
    assert "CLIENT" in customer_variants
    assert "NOM CLIENT" in customer_variants
    
    turnover_variants = COLUMN_MAPPING["TURNOVER"]
    assert "TURNOVER" in turnover_variants
    assert "CA" in turnover_variants
    assert "CHIFFRE D'AFFAIRE" in turnover_variants

def test_normalize_columns(sample_dataframes):
    """Teste la normalisation des noms de colonnes."""
    df1, df2, df3 = sample_dataframes
    
    # Normaliser les DataFrames
    df1_norm = normalize_columns(df1.copy())
    df2_norm = normalize_columns(df2.copy())
    df3_norm = normalize_columns(df3.copy())
    
    # Vérifier que les colonnes ont été normalisées
    for df in [df1_norm, df2_norm, df3_norm]:
        assert "MONTH" in df.columns
        assert "CUSTOMER NAME" in df.columns
        assert "REFERENCE" in df.columns
        assert "TURNOVER" in df.columns
        assert "QUANTITY" in df.columns
        assert "CURRENCY" in df.columns

def test_clean_and_validate_data(sample_dataframes):
    """Teste le nettoyage et la validation des données."""
    df1, df2, df3 = sample_dataframes
    
    # Normaliser puis nettoyer les DataFrames
    for i, df in enumerate([df1, df2, df3]):
        df_norm = normalize_columns(df.copy())
        df_clean = clean_and_validate_data(df_norm)
        
        # Vérifier que les dates sont converties en datetime
        assert pd.api.types.is_datetime64_dtype(df_clean["MONTH"])
        
        # Vérifier que les valeurs numériques sont correctes
        assert pd.api.types.is_numeric_dtype(df_clean["TURNOVER"])
        assert pd.api.types.is_numeric_dtype(df_clean["QUANTITY"])
        
        # Vérifier que les devises sont standardisées
        assert all(currency == currency.strip().upper() for currency in df_clean["CURRENCY"])

def test_format_month_column():
    """Teste le formatage de la colonne MONTH."""
    from ETL_SIAMP_GUI import format_month_column
    
    # Créer un DataFrame avec différents formats de mois
    df = pd.DataFrame({
        "MONTH": [1, 2, 3, "01/2023", "02/2023", "2023-01-01", "01/01/2023"]
    })
    
    # Formater avec une année spécifique
    df_formatted = format_month_column(df.copy(), 2023)
    
    # Vérifier que toutes les valeurs sont converties en datetime
    assert pd.api.types.is_datetime64_dtype(df_formatted["MONTH"])
    
    # Vérifier que les mois numériques sont correctement convertis
    first_three_months = df_formatted.iloc[:3]["MONTH"].dt.month.tolist()
    assert first_three_months == [1, 2, 3]

def test_identify_merge_keys():
    """Teste l'identification des clés de fusion."""
    from ETL_SIAMP_GUI import identify_merge_keys
    
    # DataFrame avec toutes les colonnes
    df1 = pd.DataFrame(columns=["MONTH", "CUSTOMER NAME", "REFERENCE", "TURNOVER"])
    keys1 = identify_merge_keys(df1)
    assert "MONTH" in keys1
    assert "CUSTOMER NAME" in keys1
    assert "REFERENCE" in keys1
    
    # DataFrame avec colonnes partielles
    df2 = pd.DataFrame(columns=["MONTH", "REFERENCE", "TURNOVER"])
    keys2 = identify_merge_keys(df2)
    assert "MONTH" in keys2
    assert "REFERENCE" in keys2
    assert "CUSTOMER NAME" not in keys2

if __name__ == "__main__":
    pytest.main(["-v", __file__]) 