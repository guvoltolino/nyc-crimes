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

url = "https://data.cityofnewyork.us/resource/5uac-w243.json"

response = requests.get(url)

if response.status_code == 200:
    dados = response.json()
    print(f"Dados extraidos com sucesso.")

    file_path = "/mnt/bronze/dados_nyc.json"
    
    dbutils.fs.put(file_path, json.dumps(dados), overwrite=True)
    
    print(f'Dados salvos em: {file_path}')
else:
    print(f'Falha ao obter dados. Erro: {response.status_code}')

# COMMAND ----------




