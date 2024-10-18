# Databricks notebook source
# IMPORTANDO BIBLIOTECA
from pyspark.sql.functions import to_date,first, max,col,round

# COMMAND ----------

# LENDO OS ARQUIVOS PARQUET DA CAMADA BRONZE, CRIANDO DATAFRAME COM OS DADOS DE TODOS OS ARQUIVOS DA BRONZE, E FAZENDO O PRIMEIRO PROCESSO DE TRANSFORMAÇÃO, AGRUPANDO AS DATAS E AGREGANDO OS VALORES DAS MOEDAS.
df_junto = spark.read.parquet('dbfs:/databricks-results/bronze/*/*/*')
df_moedas = df_junto.filter(df_junto.Moeda.isin(['USD','EUR','GBP']))
df_moedas = df_moedas.withColumn("data", to_date(df_moedas.data, "yyyy-MM-dd"))
resultado_taxas_conversao = df_moedas.groupBy("data").pivot("moeda").agg(first("taxa")).orderBy("data", ascending=False)


# COMMAND ----------

# CRIANDO UM DATAFRAME QUE POSSUE AS TAXAS DE CONVERSAO PARA REAL.
resultado_valores_reais =  resultado_taxas_conversao.select('*')
moedas = ['EUR','USD','GBP']
for moeda in moedas:
    resultado_valores_reais = resultado_valores_reais.withColumn(moeda,1/col(moeda))

# COMMAND ----------

# DEFINIDO O COALESCE PARA REDUZIR A QUANTIDADE DE ARQUIVOS PARA UM UNICO ARQUIVO, E GRAVANDO NO DBFS NO FORMATO CSV.
resultado_taxas_conversao = resultado_taxas_conversao.coalesce(1)
resultado_valores_reais = resultado_valores_reais.coalesce(1)
resultado_taxas_conversao.write.mode("overwrite").format("csv").option("header", "true").save("dbfs:/databricks-results/prata/taxas_conversao")
resultado_valores_reais.write.mode ("overwrite").format("csv").option("header", "true").save("dbfs:/databricks-results/ouro/valores_reais")
