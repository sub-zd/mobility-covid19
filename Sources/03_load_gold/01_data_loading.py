# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Chargement des données dans le modèle final
# MAGIC 
# MAGIC ### Description du script
# MAGIC 
# MAGIC Ce script s'occupe de charger toutes les données depuis la base de données silver, dans les _Delta Tables_ correspondantes de la base de données gold, contenant la structure du modèle final.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import des librairies nécessaires

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration de l'utilisation de la base de données silver par défaut

# COMMAND ----------

# MAGIC %sql
# MAGIC use gold_db;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Chargement de la dimension "dimGeoraphy"

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dimgeography select distinct canton_abreviation, sub_region, null from silver_db.google_data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insertion de la ville la plus peuplée du canton pour correspondre au dataset de TomTom
# MAGIC update dimgeography set most_populate_city = "Bern" where region = "Bern";
# MAGIC update dimgeography set most_populate_city = "Basel" where region = "Basel-Stadt";
# MAGIC update dimgeography set most_populate_city = "Zurich" where region = "Zurich";
# MAGIC update dimgeography set most_populate_city = "Genève" where region = "Genève";
# MAGIC update dimgeography set most_populate_city = "Lugano" where region = "Ticino";
# MAGIC update dimgeography set most_populate_city = "Lausanne" where region = "Vaud";
# MAGIC update dimgeography set most_populate_city = "NA" where most_populate_city is null;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualisation des données
# MAGIC select * from dimgeography

# COMMAND ----------

# MAGIC %md
# MAGIC #### Chargement de la dimension "dimCalendar"

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dimcalendar select date_int, date, day_of_week_char, day, month, year, vague, homeworking, school_closed, resto_closed, groceries_closed, covid_pass_active from silver_db.calendar

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualisation des données
# MAGIC select * from dimcalendar

# COMMAND ----------

# MAGIC %md
# MAGIC #### Chargement de la dimension "dimIndex"

# COMMAND ----------

# Récupération du nom des colonnes de la table all_index (fusion de touts les datasets)
df = sqlContext.sql("show columns from silver_db.all_index").toPandas()

# Récupération des index grâce au flag "i_"
df_corr = df[df['col_name'].str.startswith('i_')]
df_corr2 = df_corr['col_name'].str.slice(2)
df_corr3 = pd.DataFrame(df_corr2)

# Insertion des données dans la dimension des index
for i, row in df_corr3.iterrows():
   sqlContext.sql("insert into dimindex (id) VALUES ('"+row['col_name']+"')")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualisation des données
# MAGIC select * from dimindex

# COMMAND ----------

# MAGIC %md
# MAGIC #### Chargement de la table de fait "factIndex"

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into factindex select date_int, "grocery_and_pharmacy", canton_abreviation, i_grocery_and_pharmacy from silver_db.all_index where i_grocery_and_pharmacy is not null and i_grocery_and_pharmacy <> 0;
# MAGIC insert into factindex select date_int, "reatail_and_recreation", canton_abreviation, i_reatail_and_recreation from silver_db.all_index where i_reatail_and_recreation is not null and i_reatail_and_recreation <> 0;
# MAGIC insert into factindex select date_int, "workplaces", canton_abreviation, i_workplaces from silver_db.all_index where i_workplaces is not null and i_workplaces <> 0;
# MAGIC insert into factindex select date_int, "residential", canton_abreviation, i_residential from silver_db.all_index where i_residential is not null and i_residential <> 0;
# MAGIC insert into factindex select date_int, "walking", canton_abreviation, i_walking from silver_db.all_index where i_walking is not null and i_walking <> 0;
# MAGIC insert into factindex select date_int, "transit", canton_abreviation, i_transit from silver_db.all_index where i_transit is not null and i_transit <> 0;
# MAGIC insert into factindex select date_int, "driving", canton_abreviation, i_driving from silver_db.all_index where i_driving is not null and i_driving <> 0;
# MAGIC insert into factindex select date_int, "parks", canton_abreviation, i_parks from silver_db.all_index where i_parks is not null and i_parks <> 0;
# MAGIC insert into factindex select date_int, "congestion", canton_abreviation, i_congestion from silver_db.all_index join dimGeography on most_populate_city = city where i_congestion is not null and i_congestion <> 0;
# MAGIC 
# MAGIC insert into factindex select date_int, "new_cases", canton_abreviation, i_new_cases from silver_db.all_index where i_new_cases is not null;
# MAGIC insert into factindex select date_int, "total_cases", canton_abreviation, i_total_cases from silver_db.all_index where i_total_cases is not null;
# MAGIC insert into factindex select date_int, "new_deaths", canton_abreviation, i_new_deaths from silver_db.all_index where i_new_deaths is not null;
# MAGIC insert into factindex select date_int, "total_deaths", canton_abreviation, i_total_deaths from silver_db.all_index where i_total_deaths is not null;
# MAGIC 
# MAGIC insert into factindex select date_int, "icu_patients", canton_abreviation, i_icu_patients from silver_db.all_index where i_icu_patients is not null;
# MAGIC insert into factindex select date_int, "hosp_patients", canton_abreviation, i_hosp_patients from silver_db.all_index where i_hosp_patients is not null;
# MAGIC 
# MAGIC insert into factindex select date_int, "reproduction_rate", canton_abreviation, i_reproduction_rate from silver_db.all_index where i_reproduction_rate is not null;
# MAGIC insert into factindex select date_int, "stringency_index", canton_abreviation, i_stringency_index from silver_db.all_index where i_stringency_index is not null;
# MAGIC 
# MAGIC insert into factindex select date_int, "people_fully_vaccinated", canton_abreviation, i_people_fully_vaccinated from silver_db.all_index where i_people_fully_vaccinated is not null;
# MAGIC insert into factindex select date_int, "total_vaccinations", canton_abreviation, i_total_vaccinations from silver_db.all_index where i_total_vaccinations is not null;
# MAGIC insert into factindex select date_int, "people_vaccinated", canton_abreviation, i_people_vaccinated from silver_db.all_index where i_people_vaccinated is not null;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualisation des données
# MAGIC select * from factindex