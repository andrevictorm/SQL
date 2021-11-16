# Databricks notebook source
df = (spark.read.format("csv")
         .option("inferschema", "true")
         .option("header", "true")
         .option("sep", ";")
         .load("/FileStore/tables/arquivo_geral.csv")
         .createOrReplaceTempView("tabela_covid")
      )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tabela_covid

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regiao, estado, casosNovos FROM tabela_covid

# COMMAND ----------

# DBTITLE 1,Count()
# MAGIC %sql
# MAGIC SELECT regiao,
# MAGIC COUNT(casosNovos) as QTD_casos_NOVOS
# MAGIC FROM tabela_covid
# MAGIC GROUP BY regiao

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regiao,
# MAGIC COUNT(casosNovos) as QTD_casos_NOVOS
# MAGIC FROM tabela_covid
# MAGIC GROUP BY regiao
# MAGIC HAVING QTD_casos_NOVOS >500

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regiao, COUNT(casosNovos) AS QTD_CASOS_NOVOS 
# MAGIC FROM tabela_covid 
# MAGIC WHERE regiao == 'Nordeste' 
# MAGIC GROUP BY regiao

# COMMAND ----------

spark.sql("SELECT * FROM tabela_covid").display()
