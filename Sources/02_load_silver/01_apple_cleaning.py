# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Nettoyage des données Apple
# MAGIC 
# MAGIC ### Description du script
# MAGIC 
# MAGIC Ce script nettoie et transforme les données du dataset d'Apple pour qu'elles soient prêtes à être chargées dans la base de données gold.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import des librairies nécessaires

# COMMAND ----------

# MAGIC %run ../00_initialization/01_lib

# COMMAND ----------

from pyspark.sql.functions import col
import pandas as pd
import numpy as np
import seaborn as sns
import statsmodels.api as sm
import scipy.stats as stats
import matplotlib.pyplot as plt
%matplotlib inline
from statsmodels.graphics.gofplots import qqplot
from statsmodels.graphics.gofplots import ProbPlot

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration de l'utilisation de la base de données silver par défaut

# COMMAND ----------

# MAGIC %sql
# MAGIC use silver_db

# COMMAND ----------

# MAGIC %md
# MAGIC #### Création de la table en base de données silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create table apple_data_raw
# MAGIC using delta
# MAGIC as select sub_region, date, i_driving, i_transit, i_walking from bronze_db.apple_raw_data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standardisation du nom des cantons

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Retirer la partie "Canton of ..."
# MAGIC update apple_data_raw set sub_region = substring(sub_region, 11);
# MAGIC 
# MAGIC -- Unification du nom par rapport aux autres datasets
# MAGIC update apple_data_raw set sub_region = 'Grisons' where sub_region = 'Graubünden';
# MAGIC update apple_data_raw set sub_region = 'Genève' where sub_region = 'Geneva';
# MAGIC update apple_data_raw set sub_region = 'Zurich' where sub_region = 'Zürich';
# MAGIC update apple_data_raw set sub_region = 'Suisse' where sub_region = '';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Récupération des données en dataframe et ajout d'une colonne date en integer

# COMMAND ----------

df = sqlContext.sql("select * from apple_data_raw").toPandas()
df = df.assign(date_int = df['date'].dt.strftime('%Y%m%d').astype(int))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyse et traitement des valeurs manquantes

# COMMAND ----------

missing_values_table(df)

# COMMAND ----------

# Traitement des 0
df['i_driving'].replace(to_replace = 0, method = 'bfill', inplace = True)
df['i_walking'].replace(to_replace = 0, method = 'bfill', inplace = True)
df['i_transit'].replace(to_replace = 0, method = 'bfill', inplace = True)

# Traitement des valeurs manquantes isolées
df['i_driving'].fillna(method = 'ffill', limit = 2, inplace = True)
df['i_walking'].fillna(method = 'ffill', limit = 2, inplace = True)
df['i_transit'].fillna(method = 'ffill', limit = 2, inplace = True)

# Traitement du reste des valeurs manquantes
df['i_walking'].fillna(value = 0, inplace = True)
df['i_transit'].fillna(value = 0, inplace = True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyse et traitement des valeurs aberrantes

# COMMAND ----------

df.boxplot(figsize = (4,6), column=['i_walking', 'i_driving', 'i_transit'])

# COMMAND ----------

# Traitement des valeurs abrerrantes
limiteInfDriving = df['i_driving'].quantile(0.25) - 3 * (df['i_driving'].quantile(0.75) - df['i_driving'].quantile(0.25))
limiteSupDriving = df['i_driving'].quantile(0.75) + 3 * (df['i_driving'].quantile(0.75) - df['i_driving'].quantile(0.25))

df['i_driving'] = df['i_driving'].clip(lower = limiteInfDriving, upper = limiteSupDriving, axis = 0)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Création de la _Delta Table_

# COMMAND ----------

df_corr = spark.createDataFrame(df)
df_corr.write.format("delta").saveAsTable("apple_data")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualisation des données

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from apple_data