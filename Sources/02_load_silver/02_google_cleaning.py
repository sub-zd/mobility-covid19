# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Nettoyage des données Google
# MAGIC 
# MAGIC ### Description du script
# MAGIC 
# MAGIC Ce script nettoie et transforme les données du dataset de Google pour qu'elles soient prêtes à être chargées dans la base de données gold.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports des librairies nécessaires

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

df = sqlContext.sql("select * from bronze_db.google_raw_data").toPandas()

df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
df = df.assign(date_int = df['date'].dt.strftime('%Y%m%d').astype(int))

# Sélection des dates nécessaires après conversion
df = df[(df['date'] >= '2020-02-25') & (df['date'] <= '2022-04-12')]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyse et traitement des valeurs manquantes

# COMMAND ----------

missing_values_table(df)

# COMMAND ----------

# Traitement des 0
df['i_transit'].replace(to_replace = 0, method = 'bfill', inplace = True)
df['i_workplaces'].replace(to_replace = 0, method = 'bfill', inplace = True)
df['i_residential'].replace(to_replace = 0, method = 'bfill', inplace = True)
df['i_parks'].replace(to_replace = 0, method = 'bfill', inplace = True)
df['i_grocery_and_pharmacy'].replace(to_replace = 0, method = 'bfill', inplace = True)
df['i_reatail_and_recreation'].replace(to_replace = 0, method = 'bfill', inplace = True)

# Traitement des valeurs manquantes isolées
df['i_transit'].fillna(method ='ffill', inplace = True)
df['i_workplaces'].fillna(method ='ffill', inplace = True)
df['i_residential'].fillna(method ='ffill', limit = 2, inplace = True)
df['i_parks'].fillna(method ='ffill', limit = 2, inplace = True)
df['i_grocery_and_pharmacy'].fillna(method ='ffill', limit = 2, inplace = True)
df['i_reatail_and_recreation'].fillna(method ='ffill', limit = 2, inplace = True)

# Traitement du reste des valeurs manquantes
df['i_transit'].fillna(value = 0, inplace = True)
df['i_workplaces'].fillna(value = 0, inplace = True)
df['i_residential'].fillna(value = 0, inplace = True)
df['i_parks'].fillna(value = 0, inplace = True)
df['i_grocery_and_pharmacy'].fillna(value = 0, inplace = True)
df['i_reatail_and_recreation'].fillna(value = 0, inplace = True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyse et traitement des valeurs aberrantes

# COMMAND ----------

# Pas de traitement effectué étant donné qu'il est considéré qu'il n'y a pas de valeurs aberrantes

df.boxplot(figsize = (11,6), column=['i_grocery_and_pharmacy', 'i_residential', 'i_parks', 'i_reatail_and_recreation', 'i_transit', 'i_workplaces'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Création de la _Delta Table_

# COMMAND ----------

df_corr = spark.createDataFrame(df)
df_corr.write.format("delta").saveAsTable("google_data")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ajout de l'abréviation des cantons et standardisation du nom

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table google_data add columns (
# MAGIC   canton_abreviation string after sub_region
# MAGIC );
# MAGIC 
# MAGIC update google_data set sub_region = 'Suisse' where sub_region is null;
# MAGIC update google_data set sub_region = 'Bern' where sub_region = 'Canton of Bern';
# MAGIC update google_data set sub_region = 'Zug' where sub_region = 'Canton of Zug';
# MAGIC update google_data set sub_region = 'Basel-Stadt' where sub_region = 'Basel City';
# MAGIC update google_data set sub_region = 'Genève' where sub_region = 'Geneva';
# MAGIC 
# MAGIC update google_data set canton_abreviation = 'CH' where sub_region = 'Suisse';
# MAGIC update google_data set canton_abreviation = 'BE' where sub_region = 'Bern';
# MAGIC update google_data set canton_abreviation = 'NE' where sub_region = 'Neuchâtel';
# MAGIC update google_data set canton_abreviation = 'SZ' where sub_region = 'Schwyz';
# MAGIC update google_data set canton_abreviation = 'JU' where sub_region = 'Jura';
# MAGIC update google_data set canton_abreviation = 'VD' where sub_region = 'Vaud';
# MAGIC update google_data set canton_abreviation = 'VS' where sub_region = 'Valais';
# MAGIC update google_data set canton_abreviation = 'LU' where sub_region = 'Lucerne';
# MAGIC update google_data set canton_abreviation = 'AR' where sub_region = 'Appenzell Ausserrhoden';
# MAGIC update google_data set canton_abreviation = 'AI' where sub_region = 'Appenzell Innerrhoden';
# MAGIC update google_data set canton_abreviation = 'OW' where sub_region = 'Obwalden';
# MAGIC update google_data set canton_abreviation = 'NW' where sub_region = 'Nidwalden';
# MAGIC update google_data set canton_abreviation = 'GL' where sub_region = 'Glarus';
# MAGIC update google_data set canton_abreviation = 'FR' where sub_region = 'Fribourg';
# MAGIC update google_data set canton_abreviation = 'SO' where sub_region = 'Solothurn';
# MAGIC update google_data set canton_abreviation = 'BS' where sub_region = 'Basel-Stadt';
# MAGIC update google_data set canton_abreviation = 'BL' where sub_region = 'Basel-Landschaft';
# MAGIC update google_data set canton_abreviation = 'SH' where sub_region = 'Schaffhausen';
# MAGIC update google_data set canton_abreviation = 'GR' where sub_region = 'Grisons';
# MAGIC update google_data set canton_abreviation = 'AG' where sub_region = 'Aargau';
# MAGIC update google_data set canton_abreviation = 'TG' where sub_region = 'Thurgau';
# MAGIC update google_data set canton_abreviation = 'TI' where sub_region = 'Ticino';
# MAGIC update google_data set canton_abreviation = 'GE' where sub_region = 'Genève';
# MAGIC update google_data set canton_abreviation = 'ZG' where sub_region = 'Zug';
# MAGIC update google_data set canton_abreviation = 'SG' where sub_region = 'St. Gallen';
# MAGIC update google_data set canton_abreviation = 'UR' where sub_region = 'Uri';
# MAGIC update google_data set canton_abreviation = 'ZH' where sub_region = 'Zurich';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualisation des données

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from google_data