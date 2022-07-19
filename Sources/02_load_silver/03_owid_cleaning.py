# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Nettoyage des données OWID
# MAGIC 
# MAGIC ### Description du script
# MAGIC 
# MAGIC Ce script nettoie et transforme les données du dataset d'OWID pour qu'elles soient prêtes à être chargées dans la base de données gold.

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

df = sqlContext.sql("select * from bronze_db.owid_raw_data").toPandas()
df = df.assign(date_int = df['date'].dt.strftime('%Y%m%d').astype(int))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyse et traitement des valeurs manquantes

# COMMAND ----------

missing_values_table(df)

# COMMAND ----------

# Traitement des 0
df['i_new_cases'].replace(to_replace = 0, method = 'ffill', inplace = True)
df['i_new_deaths'].replace(to_replace = 0, method = 'ffill', inplace = True)

# Traitement des valeurs manquantes
df['i_total_vaccinations'].fillna(method ='ffill', inplace = True)
df['i_people_vaccinated'].fillna(method ='ffill', inplace = True)
df['i_people_fully_vaccinated'].fillna(method ='ffill', inplace = True)
df['i_total_vaccinations'].fillna(value = 0, inplace = True)
df['i_people_vaccinated'].fillna(value = 0, inplace = True)
df['i_people_fully_vaccinated'].fillna(value = 0, inplace = True)

df['i_icu_patients'].fillna(value = 0, inplace = True)
df['i_hosp_patients'].fillna(value = 0, inplace = True)

df['i_reproduction_rate'].fillna(method ='bfill', inplace = True)
df['i_total_deaths'].fillna(value = 0, inplace = True)

df['i_new_deaths'].fillna(method ='ffill', inplace = True)
df['i_new_deaths'].fillna(value = 0, inplace = True)
df['i_new_cases'].fillna(value = 0, inplace = True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyse et traitement des valeurs aberrantes

# COMMAND ----------

# Pas de traitement effectué étant donné qu'il est considéré qu'il n'y a pas de valeurs aberrantes

df.boxplot(figsize = (3,6), column=['i_hosp_patients']) 

# COMMAND ----------

# Pas de traitement effectué étant donné qu'il est considéré qu'il n'y a pas de valeurs aberrantes

df.boxplot(figsize = (3,6), column=['i_total_deaths']) 

# COMMAND ----------

# Pas de traitement effectué étant donné qu'il est considéré qu'il n'y a pas de valeurs aberrantes

df.boxplot(figsize = (2,6), column=['i_reproduction_rate']) 

# COMMAND ----------

# Pas de traitement effectué étant donné qu'il est considéré qu'il n'y a pas de valeurs aberrantes

df.boxplot(figsize = (2,6), column=['i_new_cases'])

# COMMAND ----------

# Pas de traitement effectué étant donné qu'il est considéré qu'il n'y a pas de valeurs aberrantes

df.boxplot(figsize = (4,6), column=[ 'i_new_deaths',  'i_stringency_index'])

# COMMAND ----------

# Pas de traitement effectué étant donné qu'il est considéré qu'il n'y a pas de valeurs aberrantes

df.boxplot(figsize = (9,6), column=['i_people_fully_vaccinated', 'i_people_vaccinated', 'i_total_vaccinations', 'i_total_cases'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Création de la _Delta Table_

# COMMAND ----------

df_corr = spark.createDataFrame(df)
df_corr.write.format("delta").saveAsTable("owid_data")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ajout de la région (données nationnales)

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table owid_data add column (
# MAGIC   region string after date
# MAGIC );
# MAGIC 
# MAGIC update owid_data set region = "Suisse";

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualisation des données

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from owid_data