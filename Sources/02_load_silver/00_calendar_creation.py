# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Création du calendrier
# MAGIC 
# MAGIC ### Description du script
# MAGIC 
# MAGIC Ce script génère un calendrier de la période de la pandémie correspondant aux données des dataset. Les données temporelles des différentes mesures prises par la Confédération y sont insérées.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports des librairies nécessaires

# COMMAND ----------

# MAGIC %run ../00_initialization/01_lib

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration de l'utilisation de la base de données silver par défaut

# COMMAND ----------

# MAGIC %sql
# MAGIC use silver_db

# COMMAND ----------

# MAGIC %md
# MAGIC #### Génération des données du calendrier et création de la _Delta Table_

# COMMAND ----------

df = spark.createDataFrame(generate_calendar('2020-02-25', '2022-04-12'))
df.write.format("delta").saveAsTable("calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ajout des colonnes correspondant aux mesures

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table calendar add columns (
# MAGIC   day_of_week_char string after day,
# MAGIC   covid_pass_active boolean after year,
# MAGIC   groceries_closed boolean after year,
# MAGIC   resto_closed boolean after year,
# MAGIC   school_closed boolean after year,
# MAGIC   homeworking boolean after year,
# MAGIC   vague string after year
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Modification du format des jours de semaine

# COMMAND ----------

# MAGIC %sql
# MAGIC update calendar set day_of_week_char = 'mo' where day_of_week = 0;
# MAGIC update calendar set day_of_week_char = 'tu' where day_of_week = 1;
# MAGIC update calendar set day_of_week_char = 'we' where day_of_week = 2;
# MAGIC update calendar set day_of_week_char = 'th' where day_of_week = 3;
# MAGIC update calendar set day_of_week_char = 'fr' where day_of_week = 4;
# MAGIC update calendar set day_of_week_char = 'sa' where day_of_week = 5;
# MAGIC update calendar set day_of_week_char = 'su' where day_of_week = 6;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insertion des dates des différentes mesures

# COMMAND ----------

# MAGIC %sql
# MAGIC update calendar set vague = '1' where date_int between 20191201 and 20200731;
# MAGIC update calendar set vague = '2' where date_int between 20200801 and 20210228;
# MAGIC update calendar set vague = '3' where date_int between 20210301 and 20210630;
# MAGIC update calendar set vague = '4' where date_int between 20210701 and 20210930;
# MAGIC update calendar set vague = '5' where date_int between 20211001 and 20220331;
# MAGIC update calendar set vague = '6' where date_int between 20220401 and 20221231;
# MAGIC 
# MAGIC update calendar set homeworking = false where date_int between 20191201 and 20210117;
# MAGIC update calendar set homeworking = true where date_int between 20210118 and 20210530;
# MAGIC update calendar set homeworking = false where date_int between 20210531 and 20211219;
# MAGIC update calendar set homeworking = true where date_int between 20211220 and 20220202;
# MAGIC update calendar set homeworking = false where date_int between 20220203 and 20221231;
# MAGIC 
# MAGIC update calendar set school_closed = false where date_int between 20191201 and 20200315;
# MAGIC update calendar set school_closed = true where date_int between 20200316 and 20200606;
# MAGIC update calendar set school_closed = false where date_int between 20200607 and 20201101;
# MAGIC update calendar set school_closed = true where date_int between 20201102 and 20210731;
# MAGIC update calendar set school_closed = false where date_int between 20210801 and 20221231;
# MAGIC 
# MAGIC update calendar set groceries_closed = false where date_int between 20191201 and 20200316;
# MAGIC update calendar set groceries_closed = true where date_int between 20200317 and 20200510;
# MAGIC update calendar set groceries_closed = false where date_int between 20200511 and 20210117;
# MAGIC update calendar set groceries_closed = true where date_int between 20210118 and 20210228;
# MAGIC update calendar set groceries_closed = false where date_int between 20210301 and 20221231;
# MAGIC 
# MAGIC update calendar set resto_closed = false where date_int between 20191201 and 20201211;
# MAGIC update calendar set resto_closed = true where date_int between 20201212 and 20210530;
# MAGIC update calendar set resto_closed = false where date_int between 20210531 and 20221231;
# MAGIC 
# MAGIC update calendar set covid_pass_active = false where date_int between 20191201 and 20210912;
# MAGIC update calendar set covid_pass_active = true where date_int between 20210913 and 20220216;
# MAGIC update calendar set covid_pass_active = false where date_int between 20220217 and 20221231;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualisation des données

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from calendar