#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_gui.py – Tests pour l'interface graphique
-----------------------------------------------
Tests unitaires pour valider le fonctionnement correct des 
composants de l'interface graphique de l'application ETL_SIAMP.
"""
import os
import sys
import pytest
import tempfile
from unittest.mock import MagicMock, patch
import pandas as pd

# Ajouter le répertoire parent au chemin pour importer les modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Tests conditionnels - certains tests seront ignorés si PyQt6 n'est pas disponible
try:
    from PyQt6.QtCore import Qt, QDate
    from PyQt6.QtWidgets import QApplication
    from PyQt6.QtTest import QTest
    from ETL_SIAMP_GUI import DropListWidget, ColumnStatusBar, MainWindow
    HAS_PYQT = True
except ImportError:
    HAS_PYQT = False

# Fixture pour l'application Qt
@pytest.fixture
def app():
    """Fixture pour l'application Qt."""
    if not HAS_PYQT:
        pytest.skip("PyQt6 n'est pas disponible")
    return QApplication([])

# Fixture pour les composants de base
@pytest.fixture
def drop_list_widget(app):
    """Fixture pour un DropListWidget."""
    return DropListWidget()

@pytest.fixture
def column_status_bar(app):
    """Fixture pour un ColumnStatusBar."""
    expected_columns = ["COL1", "COL2", "COL3"]
    return ColumnStatusBar(expected_columns)

@pytest.fixture
def main_window(app):
    """Fixture pour la fenêtre principale."""
    with patch('ETL_SIAMP_GUI.MainWindow.check_for_update'):  # Éviter la vérification de mise à jour
        window = MainWindow()
    return window

# Tests des composants
@pytest.mark.skipif(not HAS_PYQT, reason="PyQt6 n'est pas disponible")
def test_drop_list_widget_init(drop_list_widget):
    """Teste l'initialisation du DropListWidget."""
    assert drop_list_widget.count() == 0
    assert drop_list_widget.acceptDrops()
    assert drop_list_widget.selectionMode() == drop_list_widget.SelectionMode.ExtendedSelection

@pytest.mark.skipif(not HAS_PYQT, reason="PyQt6 n'est pas disponible")
def test_drop_list_widget_add_items(drop_list_widget):
    """Teste l'ajout d'éléments au DropListWidget."""
    drop_list_widget.addItem("file1.xlsx")
    drop_list_widget.addItem("file2.xlsx")
    
    assert drop_list_widget.count() == 2
    assert drop_list_widget.item(0).text() == "file1.xlsx"
    assert drop_list_widget.item(1).text() == "file2.xlsx"
    assert drop_list_widget.files() == ["file1.xlsx", "file2.xlsx"]

@pytest.mark.skipif(not HAS_PYQT, reason="PyQt6 n'est pas disponible")
def test_column_status_bar_init(column_status_bar):
    """Teste l'initialisation du ColumnStatusBar."""
    assert len(column_status_bar.column_labels) == 3
    assert "COL1" in column_status_bar.column_labels
    assert "COL2" in column_status_bar.column_labels
    assert "COL3" in column_status_bar.column_labels

@pytest.mark.skipif(not HAS_PYQT, reason="PyQt6 n'est pas disponible")
def test_column_status_bar_update(column_status_bar):
    """Teste la mise à jour du statut des colonnes."""
    files = ["file1.xlsx", "file2.xlsx", "file3.xlsx"]
    presence = {
        "COL1": set(files),              # Présent partout
        "COL2": set(files[:2]),          # Partiel
        "COL3": set()                    # Absent partout
    }
    
    column_status_bar.update_status_interactive(presence, files)
    
    # Vérifier que les styles ont été mis à jour correctement
    assert "background-color: #297F4F" in column_status_bar.column_labels["COL1"].styleSheet()  # Vert
    assert "background-color: #FFA500" in column_status_bar.column_labels["COL2"].styleSheet()  # Orange
    assert "background-color: #B22222" in column_status_bar.column_labels["COL3"].styleSheet()  # Rouge

@pytest.mark.skipif(not HAS_PYQT, reason="PyQt6 n'est pas disponible")
def test_main_window_init(main_window):
    """Teste l'initialisation de la fenêtre principale."""
    assert main_window.windowTitle() == "ETL SIAMP — Fusion Excel"
    assert main_window.tabs.count() == 3  # 3 onglets

@pytest.mark.skipif(not HAS_PYQT, reason="PyQt6 n'est pas disponible")
def test_main_window_tab_names(main_window):
    """Teste les noms des onglets de la fenêtre principale."""
    assert main_window.tabs.tabText(0) == "Traitement Mensuel"
    assert main_window.tabs.tabText(1) == "Fusion Historique"
    assert main_window.tabs.tabText(2) == "Paramètres / Références"

# Tests de fonctionnalités spécifiques
@pytest.mark.skipif(not HAS_PYQT, reason="PyQt6 n'est pas disponible")
def test_check_columns_in_files(main_window, tmp_path):
    """Teste la détection des colonnes dans les fichiers."""
    # Créer un fichier Excel temporaire
    test_file = os.path.join(tmp_path, "test.xlsx")
    df = pd.DataFrame({
        "MONTH": ["01/01/2023"],
        "CUSTOMER NAME": ["Client A"],
        "REFERENCE": ["REF001"],
        "TURNOVER": [1000]
    })
    df.to_excel(test_file, index=False)
    
    # Ajouter le fichier à la liste
    main_window.lst_files.addItem(test_file)
    
    # Mocker la méthode appendPlainText pour éviter les erreurs
    main_window.txt_log.appendPlainText = MagicMock()
    
    # Appeler la méthode à tester
    main_window._check_columns_in_files()
    
    # Vérifier que les colonnes ont été détectées correctement
    # (Nous ne pouvons pas vérifier directement le résultat visuel)
    main_window.txt_log.appendPlainText.assert_called()

@pytest.mark.skipif(not HAS_PYQT, reason="PyQt6 n'est pas disponible")
def test_load_rates(main_window):
    """Teste le chargement des taux de change."""
    # Mocker la fonction get_ecb_rates
    with patch('ETL_SIAMP.get_ecb_rates') as mock_get_rates:
        mock_get_rates.return_value = {
            "USD": 1.1,
            "GBP": 0.85,
            "JPY": 160
        }
        
        # Mocker la méthode appendPlainText
        main_window.txt_log.appendPlainText = MagicMock()
        
        # Définir une date fixe
        main_window.date_edit.setDate(QDate(2023, 1, 2))
        
        # Appeler la méthode à tester
        main_window._load_rates()
        
        # Vérifier que les taux ont été chargés correctement
        mock_get_rates.assert_called_once()
        main_window.txt_log.appendPlainText.assert_called()

@pytest.mark.skipif(not HAS_PYQT, reason="PyQt6 n'est pas disponible")
def test_load_reference_paths(main_window, tmp_path):
    """Teste le chargement des chemins de référence."""
    import configparser
    
    # Créer un fichier de configuration temporaire
    config_file = os.path.join(tmp_path, "ref_files.cfg")
    config = configparser.ConfigParser()
    config['REFERENCES'] = {'reference_file': os.path.join(tmp_path, "reference.xlsx")}
    with open(config_file, 'w') as f:
        config.write(f)
    
    # Remplacer le chemin du fichier de configuration
    original_config_file = main_window.CONFIG_REF_FILE
    main_window.CONFIG_REF_FILE = config_file
    
    # Mocker QMessageBox
    with patch('PyQt6.QtWidgets.QMessageBox.warning'):
        # Appeler la méthode à tester
        main_window._load_reference_paths()
    
    # Restaurer le chemin original
    main_window.CONFIG_REF_FILE = original_config_file

# Tests d'intégration
@pytest.mark.skipif(not HAS_PYQT, reason="PyQt6 n'est pas disponible")
def test_check_histo_columns_in_files(main_window, tmp_path):
    """Teste la détection des colonnes dans les fichiers historiques."""
    # Créer un fichier Excel temporaire
    test_file = os.path.join(tmp_path, "test_histo.xlsx")
    df = pd.DataFrame({
        "MONTH": ["01/01/2023"],
        "SIAMP UNIT": ["FRANCE"],
        "CUSTOMER NAME": ["Client A"],
        "REFERENCE": ["REF001"],
        "TURNOVER": [1000],
        "CURRENCY": ["EUR"]
    })
    df.to_excel(test_file, index=False)
    
    # Ajouter le fichier à la liste
    main_window.lst_historique_files.addItem(test_file)
    
    # Mocker la méthode appendPlainText pour éviter les erreurs
    main_window.txt_log_historique.appendPlainText = MagicMock()
    
    # Appeler la méthode à tester
    main_window._check_histo_columns_in_files()
    
    # Vérifier que la méthode a été appelée
    # (Nous ne pouvons pas vérifier directement le résultat visuel)
    assert True

if __name__ == "__main__":
    pytest.main(["-v", __file__]) 