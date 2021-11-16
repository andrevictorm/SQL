# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tabela_covid;
# MAGIC  
# MAGIC CREATE TABLE tabela_covid USING CSV
# MAGIC OPTIONS (path "/FileStore/tables/arquivo_geral.csv", header="true", delimiter=";")
# MAGIC Error in SQL statement: AnalysisException: Table default.tabela_covid already exists.
