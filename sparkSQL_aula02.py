# Databricks notebook source
Da pra usar o %sql com um DataFrame recem criado sem precisar salva-lo no Data, com o comando: df1 = df.createOrReplaceTempView('tabela_dep') Para salvar em uma tabela temporaria e poder ser puxada pelo FROM



# COMMAND ----------

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
# MAGIC SELECT *
# MAGIC FROM tabela_covid

# COMMAND ----------

# DBTITLE 1,LIKE 
- Mostrar todas as colunas da tabela_covid apenas dos estados que comecem com a letra R

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM tabela_covid
# MAGIC WHERE estado LIKE 'R%'

# COMMAND ----------

Mostrar todas as colunas da tabela_covid apenas dos estados que comecem com a letra R

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM tabela_covid
# MAGIC WHERE estado LIKE '%A'

# COMMAND ----------

Mostrar todos os dados da tabela_covid da Região Norte, apenas dos estados AM e RO



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM tabela_covid
# MAGIC WHERE regiao == "Norte" AND (estado == "AM" OR estado == "RO") 

# COMMAND ----------

Selecionar todos os Estados removendo a repetição

# COMMAND ----------

# DBTITLE 1,Utilizando o Spark.sql
spark.sql("SELECT DISTINCT(estado) FROM tabela_covid").display()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(estado)
# MAGIC FROM tabela_covid 

# COMMAND ----------

Mostrar os dados referentes aos estados de RR,AC e RN



# COMMAND ----------

# DBTITLE 1,COM OR
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM tabela_covid
# MAGIC WHERE estado == 'RR' OR estado == 'AC' OR estado == 'RN'

# COMMAND ----------

# DBTITLE 1,COM IN
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM tabela_covid
# MAGIC WHERE estado IN ('RR','AC','RN')

# COMMAND ----------

Mostrar a quantidade de casosNovos por regiao onde a quantidade for menor que 500 e ordernando por regiao de maneira decrescente



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regiao, COUNT(casosNovos) as qtd_casosNovos
# MAGIC FROM tabela_covid
# MAGIC GROUP BY regiao
# MAGIC HAVING qtd_casosNovos < 500
# MAGIC ORDER BY regiao DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tabela_covid;
# MAGIC 
# MAGIC CREATE TABLE tabela_covid USING CSV
# MAGIC OPTIONS (path "/FileStore/tables/arquivo_geral.csv", header="true", delimiter=";")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabela_covid

# COMMAND ----------



# COMMAND ----------


