# Databricks notebook source
# MAGIC %md
# MAGIC ### Couche Bronze - Ingestion des données brutes
# MAGIC Ce notebook ingère le jeu de données brutes Online Retail II depuis un volume géré vers Databricks Lakehouse.
# MAGIC Il utilise Pandas pour lire le fichier Excel, ajoute des métadonnées d'audit et enregistre les données brutes dans une table Delta (bronze_sales) pour un traitement ultérieur dans la couche Argent.

# COMMAND ----------

# Install openpyxl to enable reading Excel files with pandas
%pip install openpyxl

# Step 1: Importing pandas to read the Excel file
import pandas as pd

# Step 2: definition of the full path to the uploaded Excel file in your Databricks File System (DBFS)
file_path = "/dbfs/Volumes/e-commerce/default/e-commerce/online_retail_II.xlsx"

# Step 3: Read the Excel file using pandas
pdf = pd.read_excel(file_path)

# Step 4: Show the first 5 rows of data
display(pdf.head())


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Conversion en DataFrame Spark
# MAGIC Le fichier Excel brut a été chargé avec succès dans un DataFrame Pandas (PDF).
# MAGIC Il est maintenant converti en DataFrame Spark (df_raw) pour permettre le traitement distribué des données avec Apache Spark.
# MAGIC Cette transformation prépare l'ensemble de données pour les opérations ETL en aval et le stockage au format Delta Lake dans le cadre de la couche Bronze.
# MAGIC

# COMMAND ----------

# Convert all columns to string type to avoid mixed-type conversion issues
pdf_clean = pdf.astype(str)

# Now safely convert to Spark DataFrame
df_raw = spark.createDataFrame(pdf_clean)

# Preview the result
df_raw.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Ajouter une colonne d'audit d'ingestion
# MAGIC Pour assurer la traçabilité et le suivi de la lignée des données, une colonne supplémentaire nommée ingested_at est ajoutée à l'ensemble de données.
# MAGIC Cette colonne capture l'horodatage exact auquel les données ont été ingérées dans l'environnement Databricks.
# MAGIC L'inclusion de ces métadonnées est une pratique courante dans les pipelines d'ingénierie des données pour faciliter le débogage, le contrôle des versions et le suivi historique..
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Add an ingestion timestamp column to the Spark DataFrame
df_bronze = df_raw.withColumn("ingested_at", current_timestamp())

# Display the result with the audit column
df_bronze.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Ajuster les noms de colonnes pour la compatibilité Delta
# MAGIC Afin de respecter les contraintes techniques de Delta Lake, les noms de colonnes contenant des espaces ou des caractères spéciaux ont été ajustés.
# MAGIC Plus précisément, les espaces ont été remplacés par des traits de soulignement (par exemple, ID client → ID_client). Cela garantit la compatibilité avec les règles d'application du schéma de Delta Lake.
# MAGIC
# MAGIC Il est important de noter qu'aucune valeur de données n'a été modifiée au cours de ce processus.
# MAGIC L'ensemble de données reste une représentation fidèle des données brutes, préservant ainsi l'intégrité de la couche Bronze tout en permettant un stockage fiable au format Delta.
# MAGIC

# COMMAND ----------

# Step: Clean column names by replacing spaces with underscores
df_bronze = df_bronze.toDF(*[col.replace(" ", "_") for col in df_bronze.columns])

# Display to confirm updated column names
df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enregistrer en tant que table Delta Bronze
# MAGIC Le DataFrame Spark, désormais complété par une colonne d'horodatage ingested_at, est enregistré dans le stockage persistant sous forme de table Delta nommée bronze_sales.
# MAGIC Ceci marque l'achèvement de la couche Bronze, où les données brutes sont capturées dans leur forme originale à des fins d'auditabilité et de reproductibilité.
# MAGIC Grâce au format Delta Lake, l'ensemble de données prend en charge le contrôle de version, l'application des schémas et les fonctionnalités de voyage dans le temps.
# MAGIC

# COMMAND ----------

# Save the final DataFrame as a Delta Table (Bronze layer)
df_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_sales")
