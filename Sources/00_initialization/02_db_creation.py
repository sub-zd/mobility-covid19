# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Purge du stockage et création des bases de données
# MAGIC 
# MAGIC ### Description du script
# MAGIC 
# MAGIC Ce script permet de réinitialiser le workspace avant de créer les bases de données. En effet, comme ce sont des _Delta Tables_, des fichiers de logs sont créées et mis à jours et il n'est pas possible de créer une base de données et des nouvelles tables alors que des logs du même nom existent encore (ces fichiers de logs ne sont pas retirés à la suppression d'une base de données).

# COMMAND ----------

# MAGIC %md
# MAGIC #### Purge du dossier contenant les logs

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Suppression et création des 3 bases de données (bronze, silver, gold)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists bronze_db cascade;
# MAGIC drop database if exists silver_db cascade;
# MAGIC drop database if exists gold_db cascade;
# MAGIC 
# MAGIC create database if not exists bronze_db;
# MAGIC create database if not exists silver_db;
# MAGIC create database if not exists gold_db;