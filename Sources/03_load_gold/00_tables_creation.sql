-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Création de la structure du modèle final
-- MAGIC 
-- MAGIC ### Description du script
-- MAGIC 
-- MAGIC Ce script crée la structure de la base de données finale.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Configuration de l'utilisation de la base de données gold par défaut

-- COMMAND ----------

use gold_db

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Création des _Delta Tables_

-- COMMAND ----------

create or replace table dimIndex (
  id string not null
  )
using delta

-- COMMAND ----------

create or replace table dimGeography (
  id string not null,
  region string,
  most_populate_city string
  )
using delta

-- COMMAND ----------

create or replace table dimCalendar (
  id int not null,
  date date,
  day_of_week string, 
  day string,
  month string,
  year string,
  vague string,
  homeworking string,
  school_closed boolean,
  resto_closed boolean,
  groceries_closed boolean,
  covid_pass_active boolean
  )
using delta

-- COMMAND ----------

create or replace table factIndex (
  fk_calendar int not null,
  fk_index string not null,
  fk_geography string not null,
  value string not null
  )
using delta