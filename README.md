# ETL_SIAMP

ETL permettant d'importer, fusionner et traiter des fichiers Excel des différentes filiales afin d'en générer un complet et optimisé.

## Description

ETL_SIAMP est un outil ETL (Extract, Transform, Load) conçu pour automatiser le processus de consolidation des données Excel provenant de différentes filiales. Il permet de :

- Importer des fichiers Excel de différentes filiales
- Fusionner les données selon des règles prédéfinies
- Traiter et nettoyer les données
- Générer un fichier Excel consolidé optimisé

## Installation

1. Cloner le repository :
```bash
git clone https://github.com/EliasGhennam/ETL_SIAMP.git
```

2. Installer les dépendances :
```bash
pip install -r requirements.txt
```

## Utilisation

1. Lancer l'interface graphique :
```bash
python ETL_SIAMP_GUI.py
```

2. Suivre les instructions dans l'interface pour :
   - Sélectionner les fichiers Excel à traiter
   - Configurer les paramètres de fusion
   - Générer le fichier consolidé

## Structure du Projet

- `ETL_SIAMP_GUI.py` : Interface graphique principale
- `ETL_SIAMP.py` : Logique de traitement des données
- `requirements.txt` : Dépendances du projet
- `tests/` : Tests unitaires
- `output/` : Dossier de sortie pour les fichiers générés

## Contribution

Les contributions sont les bienvenues ! N'hésitez pas à :
1. Fork le projet
2. Créer une branche pour votre fonctionnalité
3. Commiter vos changements
4. Pousser vers la branche
5. Ouvrir une Pull Request

## Licence

Ce projet est sous licence MIT.
