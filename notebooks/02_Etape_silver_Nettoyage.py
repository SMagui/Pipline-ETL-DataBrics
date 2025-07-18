# Databricks notebook source
# MAGIC %md
# MAGIC ### Couche Argent – Nettoyage et validation des données
# MAGIC Ce bloc-notes transforme les données brutes ingérées de la couche Bronze en données nettoyées et validées dans la couche Argent.
# MAGIC Les opérations de nettoyage suivantes sont effectuées :
# MAGIC
# MAGIC Suppression des transactions annulées (numéros de facture commençant par « C »)
# MAGIC Suppression des enregistrements dont les identifiants client sont manquants ou nuls
# MAGIC Calcul d'un nouveau champ : Montant des ventes (quantité × Prix unitaire)
# MAGIC Déduplication des transactions, le cas échéant
# MAGIC Les données nettoyées sont écrites dans une table Delta nommée silver_sales, qui sert de jeu de données affiné pour l'agrégation analytique dans la couche Or
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Charger les données brutes de la table Bronze
# MAGIC L'ensemble de données brutes est lu à partir de la table Delta bronze_sales, créée précédemment et située dans la couche Bronze.
# MAGIC Cette table contient les données d'entrée brutes, ainsi qu'un horodatage d'ingestion et des noms de colonnes standardisés.
# MAGIC Les données seront ensuite filtrées et transformées lors du traitement de la couche Silver.
# MAGIC

# COMMAND ----------

# Load the raw data from the Bronze Delta table
df_bronze = spark.read.format("delta").table("bronze_sales")

# Preview the loaded DataFrame
df_bronze.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtrer les enregistrements invalides et annulés
# MAGIC Pour garantir la qualité des données, les enregistrements représentant des annulations ou manquant d'informations essentielles sont exclus.
# MAGIC Plus précisément, les transactions dont le numéro de facture commence par la lettre « C » (indiquant une annulation) sont supprimées.
# MAGIC De plus, les enregistrements dont le champ « Customer_ID » contient des valeurs nulles sont exclus, car ces entrées sont considérées comme incomplètes.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# Filter out cancelled invoices (start with 'C') and null customer IDs
df_silver = df_bronze.filter(
    (~col("Invoice").startswith("C")) &
    (col("Customer_ID").isNotNull())
)

df_silver.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Calcul du champ SalesAmount
# MAGIC Afin d'optimiser l'ensemble de données pour une utilisation analytique en aval, une nouvelle colonne nommée SalesAmount est calculée.
# MAGIC Elle est obtenue en multipliant la quantité de chaque article vendu par son prix unitaire.
# MAGIC Ce champ calculé sera utilisé pour les agrégations basées sur les revenus dans la couche Gold.
# MAGIC
# MAGIC ### Restaurer les types numériques pour le calcul
# MAGIC Comme toutes les colonnes ont été initialement converties en chaînes pour être intégrées dans la couche Bronze,
# MAGIC les champs Quantity et Price sont explicitement reconvertis dans leurs types numériques appropriés.
# MAGIC Des expressions régulières sont appliquées pour garantir que seules les lignes contenant des valeurs numériques valides sont conservées.
# MAGIC Cette étape permet d'effectuer des opérations arithmétiques précises nécessaires au calcul de SalesAmount
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# Step 3a: Filter valid numeric rows only (supporting negative quantities)
df_cleaned = df_bronze.filter(
    col("Quantity").rlike("^-?[0-9]+$") &     # e.g., -20, 100
    col("Price").rlike("^[0-9]+(\\.[0-9]+)?$") # e.g., 2.99, 10.0
)

# Step 3b: Cast string columns to proper numeric types
df_cleaned = df_cleaned.withColumn("Quantity", col("Quantity").cast("int"))
df_cleaned = df_cleaned.withColumn("Price", col("Price").cast("double"))

# Step 3c: Compute SalesAmount
df_silver = df_cleaned.withColumn("SalesAmount", col("Quantity") * col("Price"))

# Step 3d: Preview results
df_silver.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Dédupliquer les transactions (le cas échéant)
# MAGIC Afin de garantir l'intégrité des données, les doublons de transactions, le cas échéant, sont supprimés en fonction des champs pertinents.
# MAGIC La logique de déduplication utilise une combinaison de colonnes d'identification telles que Facture, Code_Stock, ID_Client et Date_Facture pour conserver les entrées uniques.
# MAGIC Cela évite les doubles comptages lors de l'agrégation dans la couche Gold.
# MAGIC

# COMMAND ----------

# Deduplicate based on common transaction identifiers
df_silver = df_silver.dropDuplicates(["Invoice", "StockCode", "Customer_ID", "InvoiceDate"])


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Enregistrer les données nettoyées dans la table Delta Silver
# MAGIC L'ensemble de données validé et enrichi est désormais enregistré dans une table Delta nommée silver_sales.
# MAGIC Cette table représente la version raffinée des données brutes et sert de source aux agrégations métier dans la couche Gold.
# MAGIC L'utilisation du format Delta préserve le contrôle des versions et l'accès efficace aux données.
# MAGIC

# COMMAND ----------

# Save cleaned and enriched data to Silver Delta table
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_sales")


# COMMAND ----------



# COMMAND ----------

# df_bronze.select("Quantity").distinct().show(100, truncate=False)
