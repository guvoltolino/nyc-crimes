# Databricks notebook source
import os
import requests 
import json

# COMMAND ----------

datalake_name = dbutils.secrets.get(scope="nyc-crimes-project", key="storage-account-name")
datalake_key = dbutils.secrets.get(scope="nyc-crimes-project", key="storage-account-key")
container_name = dbutils.secrets.get(scope="nyc-crimes-project", key="container-name")

# COMMAND ----------

if not any(mount.mountPoint == "/mnt/bronze" for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=f"wasbs://{container_name}@{datalake_name}.blob.core.windows.net/bronze",
        mount_point="/mnt/bronze",
        extra_configs={
            f"fs.azure.account.key.{datalake_name}.blob.core.windows.net": datalake_key
        }
    )
else:
    print("A montagem '/mnt/bronze' já existe.")

# COMMAND ----------

base_url = "https://data.cityofnewyork.us/resource/5uac-w243.json?$limit=1000&$offset="

total_registros = 100000 
limite = 1000  
dados_completos = []  

for offset in range(0, total_registros, limite):

    url = base_url + str(offset)
    
    response = requests.get(url)
    
    if response.status_code == 200:
        dados = response.json()
        print(f"Extraídos {len(dados)} registros com offset {offset}.")
        
        dados_completos.extend(dados)
    else:
        print(f"Falha ao obter dados. Erro: {response.status_code}")
        break 

if len(dados_completos) == total_registros:
    print(f"Todos os {total_registros} registros foram extraídos com sucesso.")
    
    file_path = "/mnt/bronze/dados_nyc.json"
    
    dbutils.fs.put(file_path, json.dumps(dados_completos), overwrite=True)
    
    print(f'Dados salvos em: {file_path}')
else:
    print(f"Erro ao extrair os registros. Total extraído: {len(dados_completos)}")
