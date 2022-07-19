# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Fusion des données
# MAGIC 
# MAGIC ### Description du script
# MAGIC 
# MAGIC Ce script fusionne certaines données afin de faciliter le chargement des dimensions en base de données gold.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import des librairies nécessaires

# COMMAND ----------

# MAGIC %run ../00_initialization/01_lib

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration de l'utilisation de la base de données silver par défaut

# COMMAND ----------

# MAGIC %sql
# MAGIC use silver_db

# COMMAND ----------

# MAGIC %md
# MAGIC #### Récupération des données nécessaires en dataframe

# COMMAND ----------

df_apple = sqlContext.sql("select * from apple_data").toPandas()
df_google = sqlContext.sql("select * from google_data").toPandas()
df_tomtom = sqlContext.sql("select * from tomtom_data").toPandas()
df_owid = sqlContext.sql("select * from owid_data").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fusion des datasets

# COMMAND ----------

# Fusion des dataset sur la date et la région
df_apple_google = df_google
df_apple_google = df_apple_google.merge(df_apple, how='inner', on=['date', 'date_int', 'sub_region'])
df_apple_google_owid = df_apple_google
df_apple_google_owid = df_apple_google_owid.merge(df_owid, how='outer', on=['date', 'date_int'])
df = df_apple_google_owid
df = df.merge(df_tomtom, how='outer', on=['date', 'date_int', 'canton_abreviation'])

# Ajout de la colonne date en integer
df = df.assign(date_int = df['date'].dt.strftime('%Y%m%d').astype(int))

# Fusion de la colonne transit en calculant la moyenne
df = df.assign(i_transit = (df['i_transit_x'] + df['i_transit_y']) / 2) 
df = df.drop('i_transit_x', axis=1)
df = df.drop('i_transit_y', axis=1)
df = df.drop('region', axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Création de la _Delta Table_ commune

# COMMAND ----------

df_corr = spark.createDataFrame(df)
df_corr.write.format("delta").saveAsTable("all_index")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mise à jour des données régionales

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE all_index SET sub_region = 'Suisse' WHERE (sub_region is null or sub_region == "") and (canton_abreviation is null or canton_abreviation == "");
# MAGIC UPDATE all_index SET canton_abreviation = 'CH' WHERE sub_region = 'Suisse';
# MAGIC UPDATE all_index SET city = 'NA' WHERE city is null;
# MAGIC UPDATE all_index SET i_congestion = 0 WHERE i_congestion is null;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualisation des données

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from all_index