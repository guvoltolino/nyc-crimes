# NYC Crimes Data Pipeline

## Descrição do Projeto

Este projeto implementa uma pipeline de dados para extrair, transformar e carregar (ETL) informações sobre crimes da cidade de Nova York a partir do NYC Open Data. Os dados são processados em três camadas: bronze, prata e ouro, e armazenados em um Data Lake no Azure.

## Estrutura do Projeto

1. **Camada Bronze**: 
   - Armazena os dados brutos extraídos em formato JSON.

2. **Camada Prata**: 
   - Contém os dados limpos e filtrados, salvos em formato Parquet. 
   - Realiza a limpeza de dados, renomeação de colunas e tratamento de valores nulos.

3. **Camada Ouro**: 
   - Dados enriquecidos prontos para análise, também salvos em formato Parquet.
   - Adiciona informações como dia da semana, mês e contagem de incidentes por bairro.

## Orquestração

A orquestração das etapas do pipeline é realizada no Azure Data Factory, permitindo agendar e monitorar a execução do fluxo de trabalho.

## Visualização

Os dados da camada ouro são utilizados para criar visualizações no Power BI, proporcionando insights sobre padrões e tendências de crimes na cidade.

## Pré-requisitos

- Azure Data Lake Storage
- Databricks
- Azure Data Factory
- Power BI

## Conclusão

Este projeto oferece uma solução robusta para análise de dados de crimes em Nova York, facilitando a obtenção de insights valiosos.
