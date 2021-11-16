# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dados1 = [
    (1, "Anderson", 10000),
    (2, "Kenedy", 20000),
    (3, "Billy", 23000),
    (4, "Andy", 23000),
    (5, "Mary", 24000),
    (6, "Eduardo", 19000),
    (7, "Mendes", 15000),
    (8, "Keyth", 18000),
    (9, "Truman", 21000),
]

schema1 = ["id", "nome", "salario"]
spark.createDataFrame(data=dados1, schema=schema1).createOrReplaceTempView("funcionarios")

dados2 = [
    (1, "Delhi", "India"),
    (2, "Tamil nadu", "India"),
    (3, "London", "UK"),
    (4, "Sydney", "Australia"),
    (8, "New York", "USA"),
    (9, "California", "USA"),
    (10, "New Jersey", "USA"),
    (11, "Texas", "USA"),
    (12, "Chicago", "USA"),
]

schema2 = ["id", "local", "pais"]
spark.createDataFrame(data=dados2, schema=schema2).createOrReplaceTempView("localizacoes")

# COMMAND ----------

SELECT campos FROM tabela1 INNER JOIN tabela 2 ON tabela1.coluna = tabela2.coluna

# COMMAND ----------

# DBTITLE 1,INNER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM funcionarios INNER JOIN localizacoes ON funcionarios.id = localizacoes.id

# COMMAND ----------

# DBTITLE 1,LEFT OUTER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM funcionarios LEFT OUTER JOIN localizacoes ON funcionarios.id = localizacoes.id

# COMMAND ----------

# DBTITLE 1,RIGHT OUTER JOIN
# MAGIC %sql
# MAGIC SELECT * FROM funcionarios RIGHT OUTER JOIN localizacoes ON funcionarios.id = localizacoes.id
