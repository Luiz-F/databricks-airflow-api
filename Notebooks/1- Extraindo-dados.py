# Databricks notebook source
# CRIANDO WIDGET PARA OBER A DATA DE EXECUÇÃO
dbutils.widgets.text('data_execucao', '')
data_execucao = dbutils.widgets.get('data_execucao')

# COMMAND ----------

# IMPORTANDO AS BIBLIOTECAS
import requests
from pyspark.sql.functions import lit

# COMMAND ----------

# FUNÇÃO QUE EXTRAI OS DADOS RAW DA API.
def extraindo_dados(date,base="BRL"):
  url = f"https://api.apilayer.com/exchangerates_data/{date}&base={base}"

  headers= {
    "apikey": "ASMA1918yABuSnkO44taLViIpz9ijKD1"
  }
  parametros = {'base':base,}
  response = requests.request("GET", url, headers=headers, params= parametros)

  if response.status_code != 200 :
    raise Exception("Erro ao capturar dados")

  return response.json()

# COMMAND ----------

# FUNÇÃO QUE CONVERTE O JSON EM UM LISTA DE TUPLAS.
def dados_para_df(dado_json):
    dados_tupla = [(moeda,float(taxa)) for moeda, taxa in dado_json['rates'].items()]
    return dados_tupla

# COMMAND ----------

# FUNÇÃO QUE CRIA O DIRETÓRIO, O DATAFRAME, ADICIONA A COLUNA DATA E SALVA NO DATABRICKS.
def salvar_arquivo_parquet(conversoes_extraidas):
    ano, mes, dia = conversoes_extraidas['date'].split('-')
    path = f'dbfs:/databricks-results/bronze/{ano}/{mes}/{dia}/'
    response = dados_para_df(conversoes_extraidas)
    df= spark.createDataFrame(response, schema=['Moeda','Taxa'])
    df = df.withColumn("data",lit(f'{ano}-{mes}-{dia}'))
    df.write.format('parquet').mode('overwrite').save(path)
    print(f'os arquivos foram salvos em {path}')



# COMMAND ----------

# EXECUTANDO AS FUNÇÕES PEGANDO A VARIÁVEL "DATA EXECUÇÃO" QUE VEM DA VARIÁVEL DE ENTRADA QUE SERÁ ALIMENTADA PELO AIRFLOW
cotacoes = extraindo_dados(data_execucao)
salvar_arquivo_parquet(cotacoes)

# COMMAND ----------


