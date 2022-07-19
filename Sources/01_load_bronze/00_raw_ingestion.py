# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Ingestion des données brutes
# MAGIC 
# MAGIC ### Description du script
# MAGIC 
# MAGIC Ce script importe les données brutes provenant des datasets importés manuellement dans le système de fichiers de Databricks via l'onglet Data : DBFS > FileStore > Tables > Upload. Quelques manipulations (expliquées au fur et à mesure) afin d'optimiser la suite du processus.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports des librairies nécessaires

# COMMAND ----------

# MAGIC %run ../00_initialization/01_lib

# COMMAND ----------

from pyspark.sql.functions import col
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration de l'utilisation de la base de données bronze par défaut

# COMMAND ----------

# MAGIC %sql
# MAGIC use bronze_db;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import des données Apple

# COMMAND ----------

# Création du dataframe depuis le fichier CSV
df = csv_import("/FileStore/tables/apple_mobility_report.csv")

# Sélection des lignes de données de type cantonale ou nationnale (les villes pour ce dataset ne sont pas utiles pour ce projet)
# Sélection des colonnes nécessaires et tri sur les données suisses et les dates
# Nom de colonne des index modifié (i_...) afin de permettre une récupération automatique des index plus tard
df = df[df.geo_type != 'city']
df_corr = df[(df['date'] >= '2020-02-25') & (df['date'] <= '2022-04-12')]
df_corr2 = df_corr.select(col('sub-region').alias('sub_region'), col('date'), col('driving').alias('i_driving'), col('transit').alias('i_transit'), col('walking').alias('i_walking')).where(df_corr.country == "Switzerland")

# Création de la Delta Table depuis ce dataset
df_corr2.write.format("delta").saveAsTable("apple_raw_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualisation des données de la nouvelle table
# MAGIC select * from apple_raw_data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import des données Google

# COMMAND ----------

# Création du dataframe depuis le fichier CSV
df = csv_import("/FileStore/tables/global_mobility_report_ch.csv")

# Sélection des colonnes nécessaires (le dataset correspond déjà aux données suisses et les dates sont modifiées dans la partie cleaning pour des raisons de format)
# Modification du nom de colonne des index (i_...) afin de permettre une récupération automatique des index plus tard
df_corr = df.select(col('sub_region_1').alias('sub_region'), col('date'), col('retail_and_recreation').alias('i_reatail_and_recreation'), col('grocery_and_pharmacy').alias('i_grocery_and_pharmacy'), col('parks').alias('i_parks'), col('transit_stations').alias('i_transit'), col('workplaces').alias('i_workplaces'), col('residential').alias('i_residential'))

# Création de la Delta Table depuis ce dataset
df_corr.write.format("delta").saveAsTable("google_raw_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualisation des données de la nouvelle table
# MAGIC select * from google_raw_data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import des données OWID (Our World in Data)

# COMMAND ----------

# Création du dataframe depuis le fichier CSV
df = csv_import("/FileStore/tables/owid_covid_data.csv")

# Sélection des colonnes nécessaires et tri sur les données suisses et les dates
# Modification du nom de colonne des index (i_...) afin de permettre une récupération automatique des index plus tard
df_corr = df[(df['date'] >= '2020-02-25') & (df['date'] <= '2022-04-12')]
df_corr2 = df_corr.select(col('date'), col('total_cases').alias('i_total_cases'), col('new_cases').alias('i_new_cases'), col('total_deaths').alias('i_total_deaths'), col('new_deaths').alias('i_new_deaths'), col('reproduction_rate').alias('i_reproduction_rate'), col('icu_patients').alias('i_icu_patients'), col('hosp_patients').alias('i_hosp_patients'), col('total_vaccinations').alias('i_total_vaccinations'), col('people_vaccinated').alias('i_people_vaccinated'), col('people_fully_vaccinated').alias('i_people_fully_vaccinated'), col('stringency_index').alias('i_stringency_index')).where(df_corr.location == "Switzerland")

# Création de la Delta Table depuis ce dataset
df_corr2.write.format("delta").saveAsTable("owid_raw_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualisation des données de la nouvelle table
# MAGIC select * from owid_raw_data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import des données TomTom

# COMMAND ----------

# Création du dataframe depuis le fichier CSV
df = csv_import("/FileStore/tables/tomtom_trafic_index.csv")

# Sélection des colonnes nécessaires et tri sur les données suisses
# Modification du nom de colonne des index (i_...) afin de permettre une récupération automatique des index plus tard
df_corr = df[(df['date'] >= '2020-02-25') & (df['date'] <= '2022-04-12')]
df_corr2 = df_corr.select(col('city'), col('date'), col('congestion').alias('i_congestion')).where(df_corr.country == "Switzerland")

# Création de la Delta Table depuis ce dataset
df_corr2.write.format("delta").saveAsTable("tomtom_raw_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualisation des données de la nouvelle table
# MAGIC select * from tomtom_raw_data