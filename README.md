
---

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





