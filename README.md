# 🧪 ETL_SIAMP – Fusion, Traitement et Visualisation de Données Excel

> Outil développé chez **SIAMP** pour automatiser la fusion de fichiers Excel, l’enrichissement métier, et l’export de données consolidées exploitables dans Power BI.

---

## 🚀 Fonctionnalités principales

- 🔄 **Fusion intelligente** de plusieurs fichiers Excel
- 📅 **Filtrage temporel** via une interface UI/UX friendly
- 💶 **Conversion automatique** : Détection et conversion des devises via les taux de la BCE
- 🧾 **Calculs enrichis** : C.A en €, Variable Costs, PRU, Margins
- 👨‍💻 **Exécutable Windows** simple d’utilisation (packagé avec PyInstaller + NSIS)
- 📊 **Fichiers finaux compatibles Power BI**

---

## 🖥️ Interface graphique

![Interface de l’application](mydata/ETL%20Siamp%20Images%20UI.png)

---

## 🧩 Technologies utilisées

| Composant         | Description                            |
|-------------------|----------------------------------------|
| Python 3.x        | Langage principal                      |
| PyQt6             | Interface graphique                    |
| Pandas / OpenPyXL | Traitement de données Excel           |
| BCE XML           | Taux de change (source officielle)     |
| Git / GitHub      | Gestion de version et collaboration    |
| PyInstaller + NSIS| Génération d’un exécutable Windows     |

---

## 📁 Structure du projet

```
ETL_SIAMP/
├── ETL_SIAMP.py             # Script principal de traitement
├── ETL_SIAMP_GUI.py         # Interface utilisateur (PyQt6)
├── fichiers_excel/          # Dossier de travail
├── output/                  # Dossier de sortie
├── build/, dist/            # (auto-générés lors du packaging)
├── installer/               # Fichiers NSIS
├── .gitignore
└── README.md
```


---

## ▶️ Lancer l’application

```bash
python ETL_SIAMP_GUI.py
```

Ou double-cliquer sur l’exécutable `ETL_SIAMP.exe` une fois installé.

---

## 🔒 Sécurité

- Les clés API sont désormais chargées depuis un fichier `.env` (non versionné)
- L’historique Git a été purgé de toute donnée sensible

---

## 📌 Auteur

Développé par **Elias Ghennam** dans le cadre d’un stage chez **SIAMP**.

---

## 📄 Licence

Projet interne à SIAMP – non destiné à une diffusion publique sans autorisation
