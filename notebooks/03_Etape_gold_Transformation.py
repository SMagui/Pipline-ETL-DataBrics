# Databricks notebook source
# MAGIC %md
# MAGIC ### Couche Or – Agrégation des ventes pour le reporting
# MAGIC Ce bloc-notes regroupe les données de transaction nettoyées de la couche Argent en indicateurs clés à des fins d'analyse.
# MAGIC Les champs suivants sont calculés :
# MAGIC
# MAGIC Ventes quotidiennes totales (TotalSales)
# MAGIC Nombre d'articles vendus par jour (TotalItems)
# MAGIC Valeur moyenne des commandes (AvgOrderValue)
# MAGIC L'ensemble de données résultant est stocké dans une table Delta Or nommée gold_sales, optimisée pour la Business Intelligence.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Charger les données nettoyées de la table Silver
# MAGIC L'ensemble de données nettoyé et validé est chargé depuis la table Delta silver_sales.
# MAGIC Cet ensemble de données sert de base à la génération d'indicateurs commerciaux agrégés dans la couche Gold.
# MAGIC

# COMMAND ----------

# Load the Silver Delta table
df_silver = spark.read.format("delta").table("silver_sales")

# Preview the data
df_silver.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Regrouper les indicateurs clés de vente par jour
# MAGIC Les indicateurs de vente au niveau de l'entreprise sont générés en agrégeant quotidiennement les données de transaction nettoyées.
# MAGIC La date de facture est convertie au format de date standard pour permettre le regroupement par date.
# MAGIC Les indicateurs suivants sont calculés pour chaque jour :
# MAGIC
# MAGIC TotalSales : chiffre d'affaires total généré
# MAGIC
# MAGIC TotalItems : nombre total d'articles vendus
# MAGIC
# MAGIC AvgOrderValue : valeur moyenne par transaction
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, to_date, col

# Extract date only from InvoiceDate string column
df_silver = df_silver.withColumn("InvoiceDate", to_date(col("InvoiceDate")))

# Group by date and compute metrics
df_gold = df_silver.groupBy("InvoiceDate").agg(
    sum("SalesAmount").alias("TotalSales"),
    sum("Quantity").alias("TotalItems"),
    (sum("SalesAmount") / countDistinct("Invoice")).alias("AvgOrderValue")
)

# Preview results
df_gold.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Enregistrer les indicateurs agrégés dans la table Delta Gold
# MAGIC L'ensemble de données agrégé final est enregistré dans une table Delta nommée gold_sales.
# MAGIC Cette table Gold fournit des indicateurs clés de l'activité dans un format optimisé pour les tableaux de bord, les rapports et la prise de décision.
# MAGIC

# COMMAND ----------

# Save the result as a Gold Delta Table
df_gold.write.format("delta").mode("overwrite").saveAsTable("gold_sales")
