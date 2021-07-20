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
"""
Por algum motivo desconhecido meu Spark não esta gerando o DF direto das listas, por isso
que gerei como pandas, salvei como csv e gerei o SparkDF do csv. 
"""
import pandas as pd 

df = pd.DataFrame({'titulo': titulo, 'classificacao': classificacao})
df.to_csv('C:\\Users\\Cliente\\Downloads\\file.csv')
df1 = spark.read.format('csv').option('header',True).load('C:\\Users\\Cliente\\Downloads\\file.csv')

#Exportando o arquivo para o storage local no formato Delta
from datetime import date

data_atual = date.today()
dat = data_atual.strftime('%Y-%m-%d')
dir = "Raw-"+dat
path = r"C:\\Users\\Cliente\\Desktop\Notebooks\\" + dir
df1.write.format("delta").mode("overwrite").save(path)

#Padronizando o nome para o dia da extração
import os

file = dat +'.parquet'
for nome in os.listdir(path):
    if nome.startswith('part') and nome.endswith('.parquet'):
        os.rename(path+"\\"+nome, path+"\\"+file)

#Upload do arquivo para o Datalake
"""
A variavel 'AZURE_STORAGE_CONNECTION_STRING' precisa ser definida com o comando abaixo no CMD

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
import pyodbc

server = 'server'
database = 'master'
username = 'username'
password = 'password'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                      server+';DATABASE='+database+';UID='+username+';PWD=' + password)
cursor = cnxn.cursor()

#Inserindo os dados no Banco de Dados
"""
A função iterrows só funciona com o Pandas. Para gravar direto do Spark precisa ser feita a conexão via JDBC
seguindo os passos em https://docs.microsoft.com/pt-br/sql/big-data-cluster/spark-mssql-connector?view=sql-server-ver15 
"""

for index, row in df.iterrows():
    cursor.execute("INSERT INTO Livros (Titulo,Classificacao) values(?,?)",
                   row.titulo, row.classificacao)
cnxn.commit()
cursor.close()