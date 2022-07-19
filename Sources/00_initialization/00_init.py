# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Initialisation
# MAGIC 
# MAGIC ### Description du script
# MAGIC 
# MAGIC Ce script d'initalisation permet de lancer le processus d'ingestion et de transformation des données jusqu'au modèle final, ainsi que l'analyse de ces données. Le but étant de ne lancer qu'un notebook pour automatiser un minimum le processus.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Amélioration de la vitesse d'exécution du processus
# MAGIC 
# MAGIC Cette partie de code commentée pourrait augmenter la vitesse d'exécution. 
# MAGIC Elle n'est pas utilisable dans ce projet car il tourne sur la version Community de Databricks, qui bloque l'exécution de workflows.

# COMMAND ----------

#dbutils.notebook.run("./01_db_creation", 60)
#dbutils.notebook.run("../01_load_bronze/00_raw_ingestion", 60)
#dbutils.notebook.run("../02_load_silver/*", 60)
#dbutils.notebook.run("../03_load_gold/*", 60)
#dbutils.notebook.run("../04_data_analysis/CRISP_Mobility-Report_TB", 60)

# COMMAND ----------

pip install cohens_d cD

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Exécution des scripts en série

# COMMAND ----------

# MAGIC %run ./02_db_creation

# COMMAND ----------

# MAGIC %run ../01_load_bronze/00_raw_ingestion

# COMMAND ----------

# MAGIC %run ../02_load_silver/00_calendar_creation

# COMMAND ----------

# MAGIC %run ../02_load_silver/01_apple_cleaning

# COMMAND ----------

# MAGIC %run ../02_load_silver/02_google_cleaning

# COMMAND ----------

# MAGIC %run ../02_load_silver/03_owid_cleaning

# COMMAND ----------

# MAGIC %run ../02_load_silver/04_tomtom_cleaning

# COMMAND ----------

# MAGIC %run ../02_load_silver/05_merge

# COMMAND ----------

# MAGIC %run ../03_load_gold/00_tables_creation

# COMMAND ----------

# MAGIC %run ../03_load_gold/01_data_loading

# COMMAND ----------

# MAGIC %run ../04_data_analysis/CRISP_Mobility-Report_TB