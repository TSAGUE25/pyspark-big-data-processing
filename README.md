# Traitement Big Data avec PySpark

> **Traitement distribué de données volumineuses pour produire des agrégats analytiques**

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Domaine](https://img.shields.io/badge/Domaine-Data%20Engineering-green)
![Statut](https://img.shields.io/badge/Statut-Portfolio-orange)
![Données](https://img.shields.io/badge/Données-Simulées%2FAnonymisées-lightgrey)

---

## Contexte métier

Lorsque les données dépassent la capacité de traitement d'un seul serveur, les outils distribués comme Apache Spark deviennent indispensables. PySpark permet de traiter des téraoctets de données avec une API Python familière.

---

## Problème traité

Traiter un dataset de plusieurs millions de lignes simulé, appliquer des transformations complexes (joins, agrégations, window functions), et comparer les performances pandas vs PySpark.

---

## Solution proposée

SparkSession local[*], broadcast join pour les petites tables, cache() pour les DataFrames réutilisés, Window Functions pour les classements, écriture Parquet partitionné par année/mois.

---

## Technologies utilisées

| Outil | Usage |
|-------|-------|
| Python 3.10+ | Langage principal |
| pandas / numpy | Manipulation des données |
| scikit-learn | Machine Learning & preprocessing |
| matplotlib / seaborn | Visualisation |
| Jupyter Notebook | Exploration interactive |

> Voir `requirements.txt` pour la liste complète.

---

## Structure du projet

```
pyspark-big-data-processing/
├── README.md              ← Ce fichier
├── PORTFOLIO.md           ← Documentation complète du cas d'usage
├── .gitignore
├── requirements.txt
├── notebooks/             ← Jupyter Notebooks d'exploration
├── src/                   ← Code Python modulaire
├── data_sample/           ← Données simulées (anonymisées)
├── figures/               ← Graphiques et visualisations
├── reports/               ← Rapports et synthèses
└── docs/                  ← Documentation complémentaire
```

---

## Installation

```bash
# 1. Cloner le dépôt
git clone https://github.com/TSAGUE25/pyspark-big-data-processing.git
cd pyspark-big-data-processing

# 2. Créer un environnement virtuel
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate    # Windows

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Lancer Jupyter
jupyter notebook
```

---

## Métriques clés (données simulées)

```
Temps d'exécution, taux de partitionnement, ratio mémoire utilisée
```

---

## Valeur métier

Scalabilité : traitement de 100M+ de lignes sans refonte du code.

---

## Limites

Mode local seulement (pas de cluster Spark). Données simulées.

---

## Prochaines améliorations

Déploiement sur Databricks ou EMR. Delta Lake pour ACID transactions.

---

## Avertissement — Confidentialité

> **Toutes les données utilisées dans ce projet sont simulées, synthétiques ou anonymisées.**
> Aucune donnée réelle, confidentielle ou propriétaire n'est présente dans ce dépôt.
> Ce projet est un cas d'usage pédagogique à destination du portfolio professionnel d'Emmanuel TSAGUE.

---

## Contributors

**TSAGUE EMMANUEL** - Data Scientist  
Specialise en Machine Learning, Data Analysis et systemes decisionnels.  
Formation Datascientest 2024 | EDF MAD EDVANCE  
Email : [emmatsague@yahoo.fr](mailto:emmatsague@yahoo.fr)  
LinkedIn : [emmanuel-tsague-114295414](https://www.linkedin.com/in/emmanuel-tsague-114295414)  
GitHub : [github.com/TSAGUE25](https://github.com/TSAGUE25)

