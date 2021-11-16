# Databricks notebook source
df = (spark.read
           .format('csv')
           .option('inferSchema','true')
           .option('header','true')
           .option('sep',';')
           .load('/FileStore/tables/arquivo_geral.csv')
           .createOrReplaceTempView('tabela_covid')
     )
 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE tabela_covid

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tabela_covid

# COMMAND ----------

Retonar a soma de casos novos por regiao, ordenando de acordo com a soma

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regiao, SUM(casosNovos) AS soma_regiao 
# MAGIC FROM tabela_covid
# MAGIC group by regiao

# COMMAND ----------

retornar a soma de casos novos, o valor mínimo de casos novos, o valor máximo de casos novos ordenados por região

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regiao, 
# MAGIC SUM(casosNovos) AS soma_regiao,
# MAGIC MIN(casosNovos) AS min_casos,
# MAGIC MAX(casosNovos) AS max_casos,
# MAGIC AVG(casosNovos) as media_casos
# MAGIC FROM tabela_covid
# MAGIC GROUP BY regiao
# MAGIC ORDER BY regiao
