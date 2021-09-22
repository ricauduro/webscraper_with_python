#WebScraperV3

#Gerando a regra de dias para pegar uma pagina por dia
from datetime import datetime

d2 = datetime.today()
d1 = datetime.strptime('2021-07-19', '%Y-%m-%d')
quantidade_dias = abs((d2 - d1).days)

#Fazendo a conexão com o site
import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen

link = "https://books.toscrape.com/catalogue/page-"+str(quantidade_dias)+".html"
response = requests.get(link)
content = response.content
soup = BeautifulSoup(content, 'html.parser')


#Gerando as listas com os dados capturados do site
titulo, classificacao = [], []

for i in soup.findAll('h3'):
    for each in i.findAll('a'):
        title = each.get('title')
        titulo.append(title)

for d in soup.find_all('p',class_='star-rating'):
    stars = d.attrs['class'][1]
    classificacao.append(stars)


#Criando a sessão do Spark
from delta import *
from pyspark.sql.session import SparkSession

builder = SparkSession.builder.appName("Rica") \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.executor.memory','8G') \
    .config('spark.driver.memory','64G') \
    .config('spark.sql.repl.eagerEval.enabled', True) \
    .config('spark.sql.repl.eagerEval.maxNumRows', 10) \
    .config('spark.driver.maxResultSize', '4G')

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()

#Criando o DataFrame
Schema = StructType([       
    StructField('title', StringType(), True),
    StructField('stars', StringType(), True)
])

df = spark.createDataFrame(zip(titulo, classificacao), Schema

#Exportando o arquivo para o storage local no formato Delta
from datetime import date

data_atual = date.today()
dat = data_atual.strftime('%Y-%m-%d')
dir = "Raw-"+dat
path = r"C:\\Users\\Cliente\\Desktop\Notebooks\\" + dir
df.write.format("delta").mode("overwrite").save(path)

#Padronizando o nome para o dia da extração
import os

file = dat +'.parquet'
for nome in os.listdir(path):
    if nome.startswith('part') and nome.endswith('.parquet'):
        os.rename(path+"\\"+nome, path+"\\"+file)

#Upload do arquivo para o Datalake
"""
A variavel de ambiente 'AZURE_STORAGE_CONNECTION_STRING' pode ser definida com o comando abaixo no PowerShell CLI

setx AZURE_STORAGE_CONNECTION_STRING "<yourconnectionstring>"

"""
from azure.storage.blob import BlobServiceClient

connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
SOURCE_FILE = path + "\\" + file
container_name = 'datalake'

def upload(SOURCE_FILE):
    blob_client = blob_service_client.get_blob_client(container_name, file)
    with open(SOURCE_FILE, 'rb') as stream:
        blob_client.upload_blob(stream, overwrite=True)

upload(SOURCE_FILE)

#Criando conexão com o SQL Server
server_name = "jdbc:sqlserver://{SERVER_ADDR}"
database_name = "master"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "livros"
username = "username"
password = "password" 

#Inserindo os dados no Banco de Dados
try:
  df.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .save()
except ValueError as error :
    print("Connector write failed", error)
