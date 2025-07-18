#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_reference_mapping.py – Tests pour le mapping des références produit
----------------------------------------------------------------------
Tests unitaires pour valider le fonctionnement correct du mapping
des références produit dans l'application ETL_SIAMP.
"""
import os
import sys
import pandas as pd
import pytest
import tempfile
import re
from unittest.mock import patch, MagicMock

# Ajouter le répertoire parent au chemin pour importer les modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import de la fonction de normalisation des références
from ETL_SIAMP import normalize_ref

@pytest.fixture
def sample_reference_data():
    """Fixture créant des données de référence pour les tests."""
    # Données pour la table de références
    table_data = pd.DataFrame({
        "REFERENCE V2": ["REF-001", "REF 002", "REF/003.A", "REF_004", "REF-005"],
        "Surfamille ret V2": ["SANITAIRE", "BATI", "CUISINE", "ROBINETTERIE", "ACCESSOIRE"]
    })
    
    # Données pour les références à enrichir
    fusion_data = pd.DataFrame({
        "REFERENCE": ["REF-001", "ref 002", "REF/003.A", "REF_004", "REF-005", "REF-006"],
        "TURNOVER": [1000, 2000, 3000, 4000, 5000, 6000],
        "QUANTITY": [10, 20, 30, 40, 50, 60]
    })
    
    return table_data, fusion_data

def test_normalize_ref_function():
    """Teste la fonction de normalisation des références."""
    test_cases = [
        ("REF-001", "REF001"),
        ("ref 002", "REF002"),
        ("REF/003.A", "REF003A"),
        ("REF_004", "REF004"),
        (None, ""),
        ("", ""),
        (123, "123")
    ]
    
    for input_ref, expected in test_cases:
        result = normalize_ref(input_ref)
        assert result == expected, f"La normalisation de {input_ref} devrait donner {expected}, mais donne {result}"

def test_reference_mapping_with_sample_data(sample_reference_data):
    """Teste le mapping des références avec des données d'exemple."""
    table_df, fusion_df = sample_reference_data
    
    # Appliquer la normalisation aux références
    fusion_df["REFERENCE_NORM"] = fusion_df["REFERENCE"].apply(normalize_ref)
    table_df["REFERENCE_V2_NORM"] = table_df["REFERENCE V2"].apply(normalize_ref)
    
    # Créer le mapping normalisé
    mapping_v2 = dict(zip(table_df["REFERENCE_V2_NORM"], table_df["Surfamille ret V2"]))
    
    # Appliquer le mapping
    fusion_df["Surfamille ret"] = fusion_df["REFERENCE_NORM"].map(mapping_v2)
    
    # Vérifier les résultats
    assert fusion_df.loc[fusion_df["REFERENCE"] == "REF-001", "Surfamille ret"].iloc[0] == "SANITAIRE"
    assert fusion_df.loc[fusion_df["REFERENCE"] == "ref 002", "Surfamille ret"].iloc[0] == "BATI"
    assert fusion_df.loc[fusion_df["REFERENCE"] == "REF/003.A", "Surfamille ret"].iloc[0] == "CUISINE"
    assert fusion_df.loc[fusion_df["REFERENCE"] == "REF_004", "Surfamille ret"].iloc[0] == "ROBINETTERIE"
    assert fusion_df.loc[fusion_df["REFERENCE"] == "REF-005", "Surfamille ret"].iloc[0] == "ACCESSOIRE"
    assert pd.isna(fusion_df.loc[fusion_df["REFERENCE"] == "REF-006", "Surfamille ret"].iloc[0])

def test_reference_fallback_mapping():
    """Teste le fallback sur un autre mapping quand la référence n'est pas trouvée."""
    # Données pour les deux mappings
    primary_mapping = {
        "REF001": "SANITAIRE V2",
        "REF002": "BATI V2",
        "REF003": "CUISINE V2"
    }
    
    fallback_mapping = {
        "REF001": "SANITAIRE",
        "REF002": "BATI",
        "REF003": "CUISINE",
        "REF004": "ROBINETTERIE",  # Uniquement dans le fallback
        "REF005": "ACCESSOIRE"     # Uniquement dans le fallback
    }
    
    # Données à enrichir
    data = pd.DataFrame({
        "REFERENCE": ["REF-001", "REF-002", "REF-003", "REF-004", "REF-005", "REF-006"],
        "REFERENCE_NORM": ["REF001", "REF002", "REF003", "REF004", "REF005", "REF006"]
    })
    
    # Appliquer le mapping principal
    data["Surfamille ret"] = data["REFERENCE_NORM"].map(primary_mapping)
    
    # Identifier les références non trouvées
    mask_vide = data["Surfamille ret"].isna()
    
    # Appliquer le fallback pour les références non trouvées
    data.loc[mask_vide, "Surfamille ret"] = data.loc[mask_vide, "REFERENCE_NORM"].map(fallback_mapping)
    
    # Vérifier les résultats
    assert data.loc[data["REFERENCE"] == "REF-001", "Surfamille ret"].iloc[0] == "SANITAIRE V2"
    assert data.loc[data["REFERENCE"] == "REF-002", "Surfamille ret"].iloc[0] == "BATI V2"
    assert data.loc[data["REFERENCE"] == "REF-003", "Surfamille ret"].iloc[0] == "CUISINE V2"
    assert data.loc[data["REFERENCE"] == "REF-004", "Surfamille ret"].iloc[0] == "ROBINETTERIE"
    assert data.loc[data["REFERENCE"] == "REF-005", "Surfamille ret"].iloc[0] == "ACCESSOIRE"
    assert pd.isna(data.loc[data["REFERENCE"] == "REF-006", "Surfamille ret"].iloc[0])

def test_reference_mapping_with_missing_values():
    """Teste le comportement du mapping avec des valeurs manquantes."""
    # Données avec valeurs manquantes
    table_data = pd.DataFrame({
        "REFERENCE V2": ["REF-001", None, "", pd.NA, "REF-005"],
        "Surfamille ret V2": ["SANITAIRE", "BATI", "CUISINE", "ROBINETTERIE", None]
    })
    
    fusion_data = pd.DataFrame({
        "REFERENCE": ["REF-001", None, "", pd.NA, "REF-005"],
        "TURNOVER": [1000, 2000, 3000, 4000, 5000]
    })
    
    # Appliquer la normalisation (avec gestion des valeurs manquantes)
    fusion_data["REFERENCE_NORM"] = fusion_data["REFERENCE"].apply(normalize_ref)
    table_data["REFERENCE_V2_NORM"] = table_data["REFERENCE V2"].apply(normalize_ref)
    
    # Créer le mapping
    mapping_v2 = dict(zip(table_data["REFERENCE_V2_NORM"], table_data["Surfamille ret V2"]))
    
    # Appliquer le mapping
    fusion_data["Surfamille ret"] = fusion_data["REFERENCE_NORM"].map(mapping_v2)
    
    # Vérifier que les valeurs manquantes sont gérées correctement
    assert fusion_data.loc[0, "Surfamille ret"] == "SANITAIRE"  # REF-001 -> SANITAIRE
    assert pd.isna(fusion_data.loc[1, "Surfamille ret"])  # None -> NaN
    assert pd.isna(fusion_data.loc[2, "Surfamille ret"])  # "" -> NaN
    assert pd.isna(fusion_data.loc[3, "Surfamille ret"])  # NA -> NaN
    assert pd.isna(fusion_data.loc[4, "Surfamille ret"])  # REF-005 -> None -> NaN

def test_customer_mapping():
    """Teste le mapping des clients vers leurs enseignes."""
    # Données de référence
    customer_data = pd.DataFrame({
        "CONCAT NAME": ["DISTRI1 Client A", "DISTRI2 Client B", "DISTRI3 Client C"],
        "Enseigne ret": ["DISTRI1", "DISTRI2", "DISTRI3"]
    })
    
    # Données à enrichir
    fusion_data = pd.DataFrame({
        "CUSTOMER NAME": ["Client A", "Client B", "Client C", "Client D"],
        "TURNOVER": [1000, 2000, 3000, 4000]
    })
    
    # Fonction pour nettoyer les chaînes
    def nettoyer_str(s):
        if pd.isna(s):
            return ""
        return str(s).strip()
    
    # Préparer les données pour le mapping
    fusion_data["CUSTOMER NAME_CLEAN"] = fusion_data["CUSTOMER NAME"].apply(nettoyer_str)
    
    # Créer les patterns de recherche pour chaque client
    patterns = {}
    for _, row in customer_data.iterrows():
        enseigne = row["Enseigne ret"]
        concat_name = row["CONCAT NAME"]
        # Extraire le nom du client (après l'enseigne)
        if enseigne in concat_name:
            client_name = concat_name.replace(enseigne, "").strip()
            patterns[client_name] = enseigne
    
    # Appliquer le mapping basé sur les patterns
    fusion_data["Enseigne ret"] = None
    for client_name, enseigne in patterns.items():
        mask = fusion_data["CUSTOMER NAME_CLEAN"] == client_name
        fusion_data.loc[mask, "Enseigne ret"] = enseigne
    
    # Vérifier les résultats
    assert fusion_data.loc[fusion_data["CUSTOMER NAME"] == "Client A", "Enseigne ret"].iloc[0] == "DISTRI1"
    assert fusion_data.loc[fusion_data["CUSTOMER NAME"] == "Client B", "Enseigne ret"].iloc[0] == "DISTRI2"
    assert fusion_data.loc[fusion_data["CUSTOMER NAME"] == "Client C", "Enseigne ret"].iloc[0] == "DISTRI3"
    assert pd.isna(fusion_data.loc[fusion_data["CUSTOMER NAME"] == "Client D", "Enseigne ret"].iloc[0])

def test_commercial_area_mapping():
    """Teste le mapping des pays vers leurs zones commerciales."""
    # Données de référence
    zone_data = pd.DataFrame({
        "PAYS": ["FRANCE", "UK", "SPAIN", "ITALY", "GERMANY"],
        "COMMERCIAL AREA": ["EUROPE", "EUROPE", "EUROPE", "EUROPE", "EUROPE"]
    })
    
    # Données à enrichir
    fusion_data = pd.DataFrame({
        "COUNTRY": ["FRANCE", "UK", "SPAIN", "ITALY", "USA"],
        "TURNOVER": [1000, 2000, 3000, 4000, 5000]
    })
    
    # Créer le mapping
    country_to_area = dict(zip(zone_data["PAYS"], zone_data["COMMERCIAL AREA"]))
    
    # Appliquer le mapping
    fusion_data["COMMERCIAL AREA"] = fusion_data["COUNTRY"].map(country_to_area)
    
    # Vérifier les résultats
    assert fusion_data.loc[fusion_data["COUNTRY"] == "FRANCE", "COMMERCIAL AREA"].iloc[0] == "EUROPE"
    assert fusion_data.loc[fusion_data["COUNTRY"] == "UK", "COMMERCIAL AREA"].iloc[0] == "EUROPE"
    assert fusion_data.loc[fusion_data["COUNTRY"] == "SPAIN", "COMMERCIAL AREA"].iloc[0] == "EUROPE"
    assert fusion_data.loc[fusion_data["COUNTRY"] == "ITALY", "COMMERCIAL AREA"].iloc[0] == "EUROPE"
    assert pd.isna(fusion_data.loc[fusion_data["COUNTRY"] == "USA", "COMMERCIAL AREA"].iloc[0])

if __name__ == "__main__":
    pytest.main(["-v", __file__]) 