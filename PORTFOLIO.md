# CAS D'USAGE 7 — PySpark & Données Volumineuses
## Traitement de grands volumes de données avec PySpark pour produire des agrégats analytiques

> **Auteur :** Emmanuel TSAGUE — Data Scientist / Data Analyst  
> **Domaine :** Data Engineering, Big Data, Traitement distribué  
> **Repository GitHub :** `pyspark-big-data-processing`  
> **Statut :** Portfolio — données simulées  
> **Date :** Juin 2026

---

## 1. TITRE ET RÉSUMÉ EXÉCUTIF

**"Traitement de données volumineuses avec PySpark — Du DataFrame local au pipeline distribué scalable"**

> **PySpark :** interface Python pour Apache Spark, le framework de traitement distribué de données le plus utilisé en entreprise. PySpark permet de traiter des volumes de données qui ne rentrent pas en mémoire RAM sur une seule machine.

> **Traitement distribué :** technique qui répartit le travail sur plusieurs machines (nœuds) en parallèle, permettant de traiter des téraoctets de données rapidement.

Une organisation traite des journaux de consommation, des logs de capteurs ou des historiques de transactions qui dépassent la capacité d'un seul ordinateur. Ce projet démontre le passage de pandas (données locales) à PySpark (données distribuées) avec les patterns analytiques essentiels.

**Résultats hypothétiques :** traitement de 50M de lignes en 45 secondes vs 8 minutes avec pandas, réduction de 70 % du temps de pipeline ETL.

---

## 2. CONTEXTE MÉTIER

> **Big Data :** terme qui désigne des volumes de données trop grands pour être traités par les outils traditionnels (Excel, pandas sur une seule machine). On parle souvent des "4V" : Volume (téraoctets), Vélocité (temps réel), Variété (formats), Véracité (qualité).

Dans les environnements énergie et industrie, les données volumineuses proviennent de :
- **Compteurs intelligents** : 35 millions de compteurs Linky en France, chacun envoyant une mesure toutes les 10 minutes = milliards de lignes par jour
- **Capteurs IoT** : des milliers de capteurs sur des équipements industriels
- **Logs applicatifs** : serveurs, SCADA, systèmes de supervision
- **Historiques transactionnels** : des années de données de facturation

> **SCADA (Supervisory Control And Data Acquisition) :** système de supervision industrielle qui collecte les données de capteurs en temps réel et permet le contrôle à distance des équipements.

> **IoT (Internet of Things — Internet des Objets) :** réseau d'objets physiques connectés qui collectent et échangent des données (capteurs, compteurs, équipements industriels).

---

## 3. POURQUOI CE SUJET EXISTE

| Raison | Limites de pandas | Solution PySpark |
|--------|------------------|-----------------|
| Volume | Dépasse la RAM (> 8-16 GB) | Distribué sur plusieurs machines |
| Vitesse | Calcul séquentiel | Parallélisation automatique |
| Scalabilité | Ne passe pas à l'échelle | Ajouter des nœuds augmente la capacité |
| Parquet | pandas lit tout en mémoire | Spark lit les colonnes utiles uniquement |
| Cloud | pandas ne profite pas du cloud | Spark tourne nativement sur Databricks, EMR |

---

## 4. PROBLÈME MÉTIER

> "Notre script Python tourne 8 heures sur un fichier de 20 Go. Il plante souvent car la RAM est dépassée. On ne peut pas traiter un an entier de données."

**Défis :**
1. Lire et traiter des fichiers de plusieurs Go sans surcharge mémoire
2. Effectuer des agrégations complexes rapidement
3. Joindre des tables volumineuses sans produit cartésien
4. Optimiser les requêtes (partitionnement, cache)
5. Exporter vers des formats analytiques optimisés (Parquet)

---

## 5. OBJECTIFS DU PROJET

| Objectif | Description | Outil |
|----------|-------------|-------|
| Lire | Charger de grands fichiers CSV et Parquet | SparkSession |
| Transformer | Nettoyage, filtrage, création de variables | DataFrame API |
| Agréger | Groupby, fenêtres, pivots | groupBy, window |
| Joindre | Jointures multi-tables efficaces | join avec broadcast |
| Optimiser | Partitionnement, cache, explain | partitionBy, cache |
| Exporter | Sauvegarder en Parquet partitionné | write.parquet |
| Comparer | Montrer la différence pandas vs PySpark | Benchmark |

---

## 6. DONNÉES SIMULÉES

```python
import pandas as pd
import numpy as np
from datetime import date, timedelta

# Génération d'un dataset de consommation simulé (représentant ~50M lignes)
# On génère un sample de 500K lignes pour le portfolio
np.random.seed(42)
N = 500_000  # 500 000 lignes pour le sample portfolio

regions    = ["Île-de-France","PACA","Bretagne","Occitanie","ARA"]
type_usage = ["Résidentiel","Tertiaire","Industriel"]
dates      = pd.date_range("2023-01-01", "2024-12-31", freq="h")

df = pd.DataFrame({
    "id_compteur":   np.random.randint(1, 50_001, N),       # 50 000 compteurs
    "timestamp":     np.random.choice(dates, N),
    "region":        np.random.choice(regions, N),
    "type_usage":    np.random.choice(type_usage, N, p=[0.6,0.3,0.1]),
    "consommation_kwh": np.abs(np.random.normal(2.5, 1.2, N)).round(3),
    "tension_v":     np.random.normal(230, 5, N).round(1),
    "qualite_mesure":np.random.choice(["OK","ESTIME","MANQUANT"],
                                       N, p=[0.85,0.10,0.05])
})

df["annee"] = df["timestamp"].dt.year
df["mois"]  = df["timestamp"].dt.month
df["heure"] = df["timestamp"].dt.hour

df.to_parquet("data_sample/consommation_sample.parquet",
              index=False, partition_cols=["annee","mois"])
print(f"Dataset créé : {len(df):,} lignes → {df.memory_usage().sum()/1e6:.1f} MB")
```

---

## 7. PYSPARK — FONDAMENTAUX ET PATTERNS

### A. Initialisation de la session Spark

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Création de la session Spark
# En local : spark tourne sur la machine (pour le développement et le portfolio)
# En production : spark tourne sur un cluster (Databricks, EMR, HDInsight)
spark = SparkSession.builder \
    .appName("ConsommationEnergie-Analytics") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version : {spark.version}")
```

> **SparkSession :** point d'entrée de toute application PySpark. Elle gère la connexion au cluster et permet de créer des DataFrames.

> **local[\*] :** mode local de Spark — il utilise tous les cœurs CPU de la machine. En production, on remplace par l'adresse du cluster.

> **Adaptive Query Execution (AQE) :** optimisation automatique des requêtes Spark au moment de l'exécution selon les statistiques réelles des données.

### B. Lecture des données

```python
# ─── Lecture CSV (avec inférence de schéma) ───────────────
df_csv = spark.read.csv(
    "data_sample/consommation_sample.csv",
    header=True,
    inferSchema=True,
    sep=","
)

# ─── Lecture Parquet (plus rapide — format colonnaire) ────
df_parquet = spark.read.parquet("data_sample/consommation_sample.parquet")

# Exploration initiale
df_parquet.printSchema()
print(f"Lignes : {df_parquet.count():,}")
df_parquet.show(5)
df_parquet.describe().show()

# ─── Lecture avec schéma explicite (meilleure pratique) ───
schema = StructType([
    StructField("id_compteur",       IntegerType(), True),
    StructField("timestamp",         TimestampType(), True),
    StructField("region",            StringType(),  True),
    StructField("type_usage",        StringType(),  True),
    StructField("consommation_kwh",  DoubleType(),  True),
    StructField("tension_v",         DoubleType(),  True),
    StructField("qualite_mesure",    StringType(),  True),
    StructField("annee",             IntegerType(), True),
    StructField("mois",              IntegerType(), True),
    StructField("heure",             IntegerType(), True),
])
df = spark.read.parquet("data_sample/consommation_sample.parquet")
```

> **Parquet :** format de fichier colonnaire binaire optimisé pour l'analytique. Il stocke les données colonne par colonne (au lieu de ligne par ligne comme CSV), permettant de lire uniquement les colonnes nécessaires — beaucoup plus rapide et plus compact.

### C. Transformations et nettoyage

```python
# ─── Nettoyage ────────────────────────────────────────────
df_clean = df \
    .filter(F.col("qualite_mesure") == "OK") \
    .filter(F.col("consommation_kwh") > 0) \
    .filter(F.col("consommation_kwh") < 100) \
    .dropDuplicates(["id_compteur", "timestamp"]) \
    .na.fill({"tension_v": 230.0})

# ─── Création de variables dérivées ───────────────────────
df_enrichi = df_clean \
    .withColumn("periode_jour",
        F.when(F.col("heure").between(6, 9),   F.lit("Matin"))
         .when(F.col("heure").between(10, 18), F.lit("Journée"))
         .when(F.col("heure").between(19, 22), F.lit("Soirée"))
         .otherwise(F.lit("Nuit"))) \
    .withColumn("tension_anomalie",
        (F.col("tension_v") < 220) | (F.col("tension_v") > 240)) \
    .withColumn("date",
        F.to_date(F.col("timestamp")))

print(f"Après nettoyage : {df_enrichi.count():,} lignes")
```

> **Lazy evaluation (évaluation paresseuse) :** PySpark ne calcule pas les transformations immédiatement — il construit un plan d'exécution. Le calcul réel n'a lieu qu'au moment d'une action (count, show, write). C'est ce qui permet l'optimisation automatique.

### D. Agrégations — groupBy

```python
# ─── Consommation mensuelle par région ────────────────────
conso_mensuelle = df_enrichi \
    .groupBy("annee", "mois", "region") \
    .agg(
        F.sum("consommation_kwh").alias("conso_totale_mwh"),
        F.avg("consommation_kwh").alias("conso_moyenne_kwh"),
        F.count("id_compteur").alias("nb_mesures"),
        F.countDistinct("id_compteur").alias("nb_compteurs_actifs"),
        F.max("consommation_kwh").alias("pic_conso_kwh")
    ) \
    .orderBy("annee", "mois", "region")

conso_mensuelle.show(10)

# ─── Profil horaire moyen ─────────────────────────────────
profil_horaire = df_enrichi \
    .groupBy("heure", "type_usage", "periode_jour") \
    .agg(F.avg("consommation_kwh").alias("conso_moy_kwh")) \
    .orderBy("type_usage", "heure")
```

### E. Fonctions fenêtres (Window Functions)

```python
from pyspark.sql.window import Window

# ─── Classement des compteurs par consommation dans chaque région ─
window_region = Window.partitionBy("region").orderBy(
    F.desc("conso_totale_mwh")
)

# D'abord agréger par compteur
conso_compteur = df_enrichi \
    .groupBy("id_compteur", "region", "type_usage") \
    .agg(F.sum("consommation_kwh").alias("conso_totale_mwh"))

# Puis classer dans chaque région
conso_compteur_rank = conso_compteur \
    .withColumn("rang_regional",
        F.rank().over(window_region)) \
    .withColumn("part_conso_region_pct",
        F.round(
            F.col("conso_totale_mwh") * 100 /
            F.sum("conso_totale_mwh").over(Window.partitionBy("region")),
            2)
    )

# Top 5 compteurs par région
conso_compteur_rank.filter(F.col("rang_regional") <= 5).show()

# ─── Variation mensuelle (LAG) ─────────────────────────────
window_time = Window.partitionBy("region").orderBy("annee", "mois")

conso_avec_variation = conso_mensuelle \
    .withColumn("conso_mois_precedent",
        F.lag("conso_totale_mwh", 1).over(window_time)) \
    .withColumn("variation_pct",
        F.round(
            (F.col("conso_totale_mwh") -
             F.col("conso_mois_precedent")) /
            F.col("conso_mois_precedent") * 100, 1))
```

### F. Jointures et broadcast

```python
# ─── Jointure avec une table de référence ─────────────────
# Table de référence : tarifs par type d'usage
tarifs_data = [
    ("Résidentiel", 0.18),
    ("Tertiaire",   0.22),
    ("Industriel",  0.12)
]
schema_tarifs = StructType([
    StructField("type_usage", StringType(), False),
    StructField("tarif_eur_kwh", DoubleType(), False)
])
df_tarifs = spark.createDataFrame(tarifs_data, schema_tarifs)

# Broadcast join : la petite table est envoyée à tous les nœuds
# Évite les shuffles coûteux sur le réseau
df_avec_tarifs = df_enrichi.join(
    F.broadcast(df_tarifs),   # ← broadcast sur la petite table
    on="type_usage",
    how="left"
).withColumn("cout_eur",
    F.round(F.col("consommation_kwh") * F.col("tarif_eur_kwh"), 4))
```

> **Broadcast join :** optimisation qui envoie la petite table à tous les nœuds du cluster pour éviter le "shuffle" (déplacement de données entre nœuds). À utiliser quand une table est < 10 MB. Cela peut réduire le temps d'une jointure de plusieurs minutes à quelques secondes.

> **Shuffle :** redistribution des données entre les nœuds du cluster lors d'opérations comme groupBy ou join. C'est l'opération la plus coûteuse en PySpark — à minimiser.

### G. Optimisations

```python
# ─── Cache : stocker un DataFrame en mémoire ──────────────
# Utile si on utilise le même DataFrame plusieurs fois
df_enrichi.cache()
df_enrichi.count()  # Action qui déclenche le calcul et le cache

# ─── Explain : voir le plan d'exécution ───────────────────
conso_mensuelle.explain(extended=False)
# Permet de voir si Spark utilise les optimisations AQE

# ─── Partitionnement pour l'export ────────────────────────
# Recommandé : 1 fichier par partition ≈ 128 MB
df_enrichi \
    .repartition("annee", "mois") \
    .write \
    .mode("overwrite") \
    .partitionBy("annee", "mois") \
    .parquet("data_sample/conso_clean_partitioned.parquet")

# ─── SQL Spark ─────────────────────────────────────────────
df_enrichi.createOrReplaceTempView("consommation")

result = spark.sql("""
    SELECT
        region,
        type_usage,
        SUM(consommation_kwh) AS conso_totale,
        COUNT(DISTINCT id_compteur) AS nb_compteurs
    FROM consommation
    WHERE qualite_mesure = 'OK'
    GROUP BY region, type_usage
    ORDER BY conso_totale DESC
""")
result.show()
```

### H. Comparaison pandas vs PySpark

```python
import time

# ─── Benchmark simplifié ───────────────────────────────────
df_pandas = df_enrichi.toPandas()  # Convertir pour comparaison

# pandas
t0 = time.time()
result_pandas = df_pandas.groupby(["region","type_usage"])["consommation_kwh"].sum()
t_pandas = time.time() - t0

# PySpark
t0 = time.time()
result_spark = df_enrichi.groupBy("region","type_usage") \
    .agg(F.sum("consommation_kwh")).collect()
t_spark = time.time() - t0

print(f"pandas : {t_pandas:.2f}s | PySpark : {t_spark:.2f}s")
print(f"Gain   : ×{t_pandas/t_spark:.1f}")
```

---

## 8. PATTERNS AVANCÉS

### SQL Spark et sous-requêtes

```sql
-- Spark SQL supporte la majorité du SQL ANSI
-- Utile pour les équipes SQL qui ne connaissent pas l'API DataFrame

WITH conso_region AS (
    SELECT
        region,
        DATE_FORMAT(timestamp, 'yyyy-MM') AS mois,
        SUM(consommation_kwh)             AS conso_mwh
    FROM consommation
    WHERE qualite_mesure = 'OK'
    GROUP BY region, DATE_FORMAT(timestamp, 'yyyy-MM')
),
stats AS (
    SELECT region,
           AVG(conso_mwh)  AS moy_mensuelle,
           STDDEV(conso_mwh) AS ecart_type
    FROM conso_region
    GROUP BY region
)
SELECT
    c.region, c.mois, c.conso_mwh,
    ROUND((c.conso_mwh - s.moy_mensuelle) / s.ecart_type, 2) AS z_score
FROM conso_region c
JOIN stats s USING (region)
HAVING ABS(z_score) > 2
ORDER BY ABS(z_score) DESC
```

---

## 9. DÉMARCHE ÉTAPE PAR ÉTAPE

```
ÉTAPE 1 : Initialiser SparkSession (local ou cluster)
ÉTAPE 2 : Lire les données (CSV ou Parquet avec schéma explicite)
ÉTAPE 3 : Explorer (printSchema, show, describe, count)
ÉTAPE 4 : Nettoyer (filter, dropDuplicates, na.fill)
ÉTAPE 5 : Transformer (withColumn, when, to_date)
ÉTAPE 6 : Agréger (groupBy, agg)
ÉTAPE 7 : Fenêtres et rankings (Window, rank, lag)
ÉTAPE 8 : Jointures (broadcast pour petites tables)
ÉTAPE 9 : Optimiser (cache, repartition, explain)
ÉTAPE 10 : Exporter (write.parquet avec partitionnement)
```

---

## 10. MÉTRIQUES

| Métrique | pandas | PySpark | Contexte |
|----------|--------|---------|----------|
| Temps de chargement | 45 s | 8 s | Fichier 5 Go |
| Temps groupBy | 12 s | 2 s | 50M lignes, 5 groupes |
| Mémoire utilisée | 12 GB | 2 GB (distribué) | 50M lignes |
| Jointure 50M × 1M | Plante | 25 s | Avec broadcast |
| Export Parquet | 3 s | 1 s (parallèle) | 5 Go |

---

## 11. RÉSULTATS SIMULÉS

| Indicateur | Valeur |
|-----------|--------|
| Volume traité | 50M lignes (simulé) |
| Temps pipeline pandas | 8 min 20 s |
| Temps pipeline PySpark | 45 s |
| Gain de performance | ×11 |
| Lignes filtrées (qualité) | 7,3 % |
| Export Parquet compressé | 68 % de réduction vs CSV |

---

## 12. VALEUR MÉTIER

| Valeur | Description |
|--------|-------------|
| **Scalabilité** | Le même code traite 500K ou 500M lignes |
| **Rapidité** | Les analyses journalières passent de la nuit au temps réel |
| **Coût cloud** | Moins de temps de calcul = moins de coût sur Databricks/EMR |
| **Fiabilité** | PySpark gère les pannes de nœuds automatiquement |
| **Compatibilité** | Le code PySpark tourne sur Databricks, AWS EMR, Azure HDInsight |

---

## 13. LIMITES

| Limite | Description |
|--------|-------------|
| Complexité de setup | Un cluster Spark est plus difficile à configurer que Python local |
| Overhead pour petits datasets | Pour < 1M lignes, pandas est souvent plus simple et rapide |
| Débogage | Les erreurs dans les transformations apparaissent à l'exécution, pas à la définition |
| Mémoire driver | Le nœud driver peut manquer de mémoire si on collecte trop de données |
| Coût cluster | Un cluster cloud coûte de l'argent — à éteindre quand non utilisé |

---

## 14. AMÉLIORATIONS

- Databricks : environnement cloud managé, notebooks collaboratifs
- Delta Lake : format transactionnel sur Parquet (ACID, versionnement)
- Spark Structured Streaming : traitement en temps réel
- MLlib : ML distribué avec Spark
- Koalas / pandas API on Spark : API pandas sur Spark (migration facilitée)

> **Delta Lake :** couche de stockage open-source au-dessus de Parquet qui ajoute les transactions ACID, le versionnement des données (time travel) et l'audit. Utilisé nativement sur Databricks.

---

## 15. ARCHITECTURE GITHUB

```
pyspark-big-data-processing/
├── README.md
├── requirements.txt
├── data_sample/
│   ├── generate_sample_data.py
│   └── consommation_sample.parquet
├── notebooks/
│   ├── 01_pyspark_fondamentaux.ipynb
│   ├── 02_transformations.ipynb
│   ├── 03_agregations_fenetres.ipynb
│   ├── 04_jointures_optimisation.ipynb
│   └── 05_benchmark_pandas_spark.ipynb
├── src/
│   ├── spark_session.py
│   ├── pipeline_conso.py
│   └── quality_checks_spark.py
├── reports/
│   └── benchmark_results.md
└── docs/
    └── guide_pyspark_debutant.md
```

---

## 16. README GITHUB

```markdown
# PySpark Big Data Processing
## Traitement de données volumineuses avec PySpark pour l'analytique énergétique

> **Auteur :** Emmanuel TSAGUE | **Données :** simulées (consommation énergie)

## Objectif
Démontrer le passage de pandas à PySpark pour traiter des volumes de données
qui dépassent la capacité d'un seul ordinateur.

## Patterns démontrés
SparkSession · CSV/Parquet · transformations · groupBy · Window Functions
LAG · Broadcast Join · Cache · Parquet partitionné · SQL Spark

## Benchmark (simulé)
pandas : 8 min 20 s → PySpark : 45 s (×11 plus rapide sur 50M lignes)

## Avertissement
Données entièrement simulées — représentatives d'un cas réel d'exploitation.
```

---

## 17. VERSION CV

> Traitement de données volumineuses avec PySpark : lecture CSV/Parquet avec schéma explicite, transformations (withColumn, when, filter), agrégations (groupBy, agg), fonctions fenêtres (rank, lag, partitionBy), jointures broadcast, optimisation (cache, repartition, AQE), export Parquet partitionné — Python, PySpark, Spark SQL.

---

## 18. VERSION ENTRETIEN

"J'ai travaillé sur un cas de traitement de données volumineuses avec PySpark, dans un contexte de consommation énergétique avec 50 millions de mesures. Le problème : le script pandas plantait par manque de mémoire et prenait 8 minutes même quand il fonctionnait. J'ai migré le pipeline vers PySpark en trois étapes : d'abord la lecture en Parquet avec schéma explicite plutôt que CSV ; ensuite les transformations et agrégations avec l'API DataFrame ; enfin l'optimisation avec le broadcast join pour la jointure avec la table de tarifs, et le partitionnement Parquet par année/mois pour les lectures filtrées. Le résultat simulé : de 8 minutes à 45 secondes. La principale leçon : PySpark est plus complexe que pandas mais c'est la seule option viable quand les données dépassent la RAM disponible."

---

## 19. VERSION PORTFOLIO

Ce projet démontre la connaissance du Big Data Engineering dans un contexte de traitement de données massives. PySpark est le standard industriel pour ce type de workload. La capacité à écrire du code efficace (broadcast join, cache, partitionnement) est ce qui distingue un Data Engineer qui "connaît PySpark" d'un qui sait réellement l'optimiser.

---

## 20. POST LINKEDIN

**Quand pandas ne suffit plus.**

8 millions de mesures de capteurs. Un script pandas qui tourne 8 minutes. Et qui plante parfois par manque de mémoire.

La solution : PySpark.

Le même pipeline, réécrit avec PySpark, tourne en 45 secondes (simulé) — soit 11 fois plus rapide.

Mais la vraie valeur de PySpark n'est pas juste la vitesse : c'est la scalabilité. Le même code qui tourne sur 500 000 lignes en local tournera sur 500 millions de lignes sur un cluster Databricks ou AWS EMR, sans modifier une seule ligne.

Quelques patterns clés que j'ai démontrés dans ce cas :

🔹 Broadcast join : envoyer une petite table à tous les nœuds pour éviter les shuffles coûteux
🔹 Parquet partitionné : lire uniquement les partitions nécessaires
🔹 Window Functions : LAG et RANK distribués sur des millions de lignes
🔹 Cache : stocker en mémoire un DataFrame réutilisé plusieurs fois

`#PySpark` `#BigData` `#DataEngineering` `#Python` `#Spark` `#Databricks`

---

## 21. QUESTIONS D'ENTRETIEN

**Q : Quelle différence entre transformation et action en PySpark ?**
> Une transformation (filter, groupBy, withColumn) ne déclenche pas de calcul — Spark construit un plan d'exécution. Une action (count, show, collect, write) déclenche l'exécution réelle. C'est la "lazy evaluation". L'avantage : Spark peut optimiser l'ensemble du plan avant de l'exécuter.

**Q : Quand utiliser le cache en PySpark ?**
> Quand on utilise le même DataFrame plusieurs fois dans le pipeline. Sans cache, Spark recalcule le DataFrame depuis le début à chaque action. Avec cache, il est stocké en mémoire. Attention : le cache coûte de la mémoire — ne pas cacher des DataFrames très volumineux inutilement.

**Q : Qu'est-ce qu'un shuffle et comment le minimiser ?**
> Un shuffle est la redistribution des données entre les nœuds lors des groupBy ou join. C'est l'opération la plus coûteuse. Pour le minimiser : utiliser broadcast join pour les petites tables, filtrer le plus tôt possible dans le pipeline, utiliser des clés de partitionnement bien choisies.

**Q : Quelle est la différence entre Parquet et CSV pour le Big Data ?**
> CSV est un format texte ligne par ligne — lisible par humain mais lent à lire. Parquet est un format binaire colonnaire — illisible directement mais 10× plus rapide en lecture (lit uniquement les colonnes utiles) et 5× plus compact grâce à la compression par colonne.

---

## 22-23. COMPÉTENCES DÉMONTRÉES

| Compétence | Preuve | Valeur | Phrase CV |
|-----------|--------|--------|-----------|
| PySpark DataFrame API | Transformations, agrégations, fenêtres | Traitement Big Data | "Pipeline PySpark : groupBy, Window, broadcast join" |
| Parquet | Lecture/écriture, partitionnement | Performance ×10 | "Format Parquet partitionné pour analytique distribuée" |
| Optimisation Spark | Cache, AQE, broadcast, repartition | Coût cloud réduit | "Optimisation Spark : broadcast, cache, partitionnement" |
| Spark SQL | SQL ANSI sur DataFrames Spark | Accessibilité équipe | "Spark SQL : requêtes analytiques sur grands volumes" |
| Benchmark | Comparaison pandas vs PySpark | Argumenter les choix | "Benchmark et analyse de performance pandas/PySpark" |

---

*Fin du document — Emmanuel TSAGUE — CAS 7 — PySpark & Données Volumineuses*
