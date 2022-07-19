# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Nettoyage des données TomTom
# MAGIC 
# MAGIC ### Description du script
# MAGIC 
# MAGIC Ce script nettoie et transforme les données du dataset de TomTom pour qu'elles soient prêtes à être chargées dans la base de données gold.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import des librairies nécessaires

# COMMAND ----------

# MAGIC %run ../00_initialization/01_lib

# COMMAND ----------

from pyspark.sql.functions import col
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration de l'utilisation de la base de données silver par défaut

# COMMAND ----------

# MAGIC %sql
# MAGIC use silver_db

# COMMAND ----------

# MAGIC %md
# MAGIC #### Récupération des données en dataframe et ajout d'une colonne date en integer

# COMMAND ----------

df = sqlContext.sql("select * from bronze_db.tomtom_raw_data").toPandas()
df = df.assign(date_int = df['date'].dt.strftime('%Y%m%d').astype(int))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyse et traitement des valeurs manquantes

# COMMAND ----------

# Pas de traitement effectué étant donné qu'il n'y a pas de valeurs manquantes ou à 0

missing_values_table(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyse et traitement des valeurs aberrantes

# COMMAND ----------

# Pas de traitement effectué étant donné qu'il est considéré qu'il n'y a pas de valeurs aberrantes

df.boxplot(figsize = (2,6), column=['i_congestion'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Création de la _Delta Table_

# COMMAND ----------

df_corr = spark.createDataFrame(df)
df_corr.write.format("delta").saveAsTable("tomtom_data")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ajout des région et définition de la ville

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table tomtom_data add columns (
# MAGIC   canton_abreviation string first
# MAGIC );
# MAGIC 
# MAGIC update tomtom_data set city = 'Genève' where city = 'Geneva';
# MAGIC update tomtom_data set canton_abreviation = 'BE' where city = 'Bern';
# MAGIC update tomtom_data set canton_abreviation = 'VD' where city = 'Lausanne';
# MAGIC update tomtom_data set canton_abreviation = 'ZH' where city = 'Zurich';
# MAGIC update tomtom_data set canton_abreviation = 'TI' where city = 'Lugano';
# MAGIC update tomtom_data set canton_abreviation = 'GE' where city = 'Geneva';
# MAGIC update tomtom_data set canton_abreviation = 'BS' where city = 'Basel';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualisation des données

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tomtom_data