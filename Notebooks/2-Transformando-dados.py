# Databricks notebook source
from pyspark.sql.functions import to_date,first, max,col,round
df_junto = spark.read.parquet('dbfs:/databricks-results/bronze/*/*/*')
df_moedas = df_junto.filter(df_junto.Moeda.isin(['USD','EUR','GBP']))
df_moedas = df_moedas.withColumn("data", to_date(df_moedas.data, "yyyy-MM-dd"))
resultado_taxas_conversao = df_moedas.groupBy("data").pivot("moeda").agg(first("taxa")).orderBy("data", ascending=False)
resultado_valores_reais =  resultado_taxas_conversao.select('*')
moedas = ['EUR','USD','GBP']
for moeda in moedas:
    resultado_valores_reais = resultado_valores_reais.withColumn(moeda,1/col(moeda))

resultado_taxas_conversao = resultado_taxas_conversao.coalesce(1)
resultado_valores_reais = resultado_valores_reais.coalesce(1)
resultado_taxas_conversao.write.mode("overwrite").format("csv").option("header", "true").save("dbfs:/databricks-results/prata/taxas_conversao")
resultado_valores_reais.write.mode ("overwrite").format("csv").option("header", "true").save("dbfs:/databricks-results/prata/valores_reais")

# COMMAND ----------

#dbutils.fs.ls('dbfs:/databricks-results/prata/valores_reais/')
display(spark.read.csv('dbfs:/databricks-results/prata/valores_reais/*',header=True),header=True)

# COMMAND ----------


