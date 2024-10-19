# Databricks notebook source

#  CONEXÃO COM O DATALAKE.
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "",
          "fs.azure.account.oauth2.client.secret": "",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/39d95a3b-2b14-4125-8155-c31fc74e84b3/oauth2/token"}

# COMMAND ----------

#dbutils.fs.mount(
#  source = "abfss://taxas@datalakeestudo.dfs.core.windows.net/",
#  mount_point = "/mnt/dados/",
#  extra_configs = configs)

# COMMAND ----------

# CARREGANDO A CAMADA BRONZE EM CSV
bronze = spark.read.parquet('dbfs:/databricks-results/bronze/*/*/*/')
bronze.coalesce(1).write.mode("overwrite").csv('dbfs:/mnt/dados/bronze/',header=True)

# CARREGANDO A CAMADA SILVER EM CSV
prata = spark.read.csv('dbfs:/databricks-results/prata/taxas_conversao/')
prata.coalesce(1).write.mode("overwrite").csv('dbfs:/mnt/dados/prata',header=True)

# CARREGANDO A CAMADA GOLD EM CSV
ouro = spark.read.csv('dbfs:/databricks-results/ouro/valores_reais/')
ouro.coalesce(1).write.mode("overwrite").csv('dbfs:/mnt/dados/ouro/',header=True)

# COMMAND ----------


