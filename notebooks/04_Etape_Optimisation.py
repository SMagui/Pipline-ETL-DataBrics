# Databricks notebook source
# MAGIC %md
# MAGIC ### optimisation_schedule
# MAGIC Objectif :
# MAGIC Ce bloc-notes sert de couche d’orchestration du pipeline de données. Il exécute l’intégralité du flux ETL, de l’ingestion des données brutes aux informations agrégées, et applique les optimisations de performances après traitement.
# MAGIC
# MAGIC ### Principales responsabilités :
# MAGIC Déclenchement des blocs-notes en amont
# MAGIC
# MAGIC Exécution séquentielle des blocs-notes de transformation Bronze, Argent et Or à l’aide de dbutils.notebook.run.
# MAGIC Optimisation des tables Delta
# MAGIC
# MAGIC ### Application des commandes de performance Delta Lake, telles que :
# MAGIC OPTIMIZE pour compacter les petits fichiers ;
# MAGIC ZORDER BY pour améliorer les performances des requêtes pour des champs spécifiques ;
# MAGIC VACUUM pour nettoyer les fichiers obsolètes et réduire les coûts de stockage ;
# MAGIC Simulation d’une exécution de production quotidienne
# MAGIC
# MAGIC ### Représente un planificateur de tâches réel tel que Databricks Workflows, Airflow ou CRON.
# MAGIC Garantie de l’exécution fiable et répétée du pipeline de bout en bout.
# MAGIC Résultat :
# MAGIC L’exécution de ce bloc-notes permet de mettre en place un pipeline de données complet et optimisé, garantissant la disponibilité d’ensembles de données propres, organisés et performants pour les analyses en aval ou les tableaux de bord BI.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Charger les tables Bronze, Argent et Or
# MAGIC Chaque table Delta du pipeline ETL est chargée pour valider son existence et sa structure avant l'optimisation.
# MAGIC

# COMMAND ----------

df_bronze = spark.read.table("bronze_sales")
df_silver = spark.read.table("silver_sales")
df_gold = spark.read.table("gold_sales")

df_gold.display()  # Just previewing one layer


# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimiser et classer les tables par ordre de ZORDER
# MAGIC La commande OPTIMIZE permet de compacter les petits fichiers et d'améliorer les performances de lecture.
# MAGIC La commande ZORDER BY regroupe ensuite les fichiers en fonction d'une colonne de filtre telle que InvoiceDate.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold_sales ZORDER BY (InvoiceDate)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vider pour récupérer de l'espace de stockage
# MAGIC Pour réduire les coûts de stockage, les anciens fichiers de données inutiles au voyage dans le temps sont supprimés à l'aide de la commande VACUUM.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC
# MAGIC VACUUM gold_sales RETAIN 168 HOURS
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Historique de la table Delta et voyage dans le temps
# MAGIC Delta Lake conserve l'historique des versions et permet de revenir aux versions précédentes grâce au numéro de version ou à l'horodatage.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold_sales;
# MAGIC
# MAGIC -- Example rollback preview
# MAGIC SELECT * FROM gold_sales VERSION AS OF 0;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulation de la planification des tâches
# MAGIC Chaque bloc-notes du pipeline peut être chaîné par programmation à l'aide de %run pour simuler l'exécution planifiée.
# MAGIC

# COMMAND ----------

# Simulate daily run
dbutils.notebook.run("01_bronze_ingestion", 300)
dbutils.notebook.run("02_silver_cleaning", 300)
dbutils.notebook.run("03_gold_aggregation", 300)
