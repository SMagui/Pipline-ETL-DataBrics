# 🧪 Pipeline ETL avec Databricks – Ventes E-commerce

Ce projet illustre la création complète d’un pipeline **ETL (Extract, Transform, Load)** utilisant **Databricks**, **PySpark** et **Delta Lake**, en suivant l’architecture **Bronze → Silver → Gold**.  
Il traite un jeu de données de ventes e-commerce provenant du monde réel : le [Online Retail II dataset](https://archive.ics.uci.edu/ml/datasets/Online+Retail+II).

---

## 📦 Jeu de Données

- **Source :** UCI Machine Learning Repository  
- **Format :** Excel (`.xlsx`)  
- **Champs :** `Invoice`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `Price`, `Customer_ID`, `Country`

---

## 🏗️ Architecture du Pipeline

| Couche        | Description |
|---------------|-------------|
| 🟫 **Bronze** | Ingestion brute + horodatage d’audit |
| ⚪ **Silver** | Nettoyage, dédoublonnage, typage, enrichissement |
| 🟨 **Gold**   | Agrégation des indicateurs journaliers (chiffre d'affaires, volume, AOV) |

---

## ⚙️ Technologies Utilisées

- 🧠 **Databricks** (Community Edition compatible)
- 🐍 **PySpark** / Spark SQL
- 💾 **Delta Lake** (gestion des versions, Z-Ordering, validation de schéma)
- 🔧 **Git & GitHub** pour le versionnement

---

## 📁 Structure du Répertoire
project-root/
├── 1_Etape_Bronze_Ingestion.ipynb # Ingestion brute
├── 02_Etape_silver_Nettoyage.ipynb # Nettoyage & transformation
├── 03_Etape_gold_Transformation.ipynb # Agrégation des KPIs
├── data/
│ └── online_retail_II.xlsx # Jeu de données source
└── README.md


## 📊 Indicateurs calculés (Gold Layer)

| Indicateur        | Description                                 |
|-------------------|---------------------------------------------|
| `TotalSales`      | Somme du chiffre d'affaires par jour        |
| `TotalItems`      | Nombre total d’articles vendus par jour     |
| `AvgOrderValue`   | Valeur moyenne d’une commande (CA / nb factures) |

---

## ▶️ Comment exécuter ce pipeline

1. Importez les notebooks dans **Databricks**
2. Téléversez le fichier `online_retail_II.xlsx` via l’interface `Data > Add Data`
3. Suivez l’ordre d’exécution :
   - `01_bronze_ingestion.py`
   - `02_silver_cleaning.py`
   - `03_gold_aggregation.py`
4. Visualisez les résultats via `display()` ou créez un dashboard

---

## 📈 Exemple de visualisations possibles

- Volume de ventes journalier 📊  
- Chiffre d’affaires par pays 🌍  
- Évolution de la valeur moyenne de commande 🧾

---

## 👤 Auteur

Réalisé par **MAGUIRAGA SEKOU** dans le cadre d'une démonstration professionnelle d’ingénierie des données.




