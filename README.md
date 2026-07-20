# PS - LED 2026
# 🚖 NYC Taxi Data Pipeline: Arquitetura Lakehouse com ELT Resiliente

[![Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![GitHub](https://img.shields.io/badge/DataOps-Resilience-green?style=for-the-badge)]()

Este projeto implementa um pipeline de dados robusto no padrão **ELT (Extract, Load, Transform)** utilizando a arquitetura **Medalhão (Bronze -> Silver -> Gold)**. O objetivo principal é extrair dados históricos de corridas de táxi de Nova York (NYC TLC), garantindo resiliência de rede na ingestão e alta performance no processamento distribuído.

---

## 🏗️ Desenho da Arquitetura

O pipeline foi desenhado sob as premissas modernas de Engenharia de Dados (DataOps), focando na separação estrita de responsabilidades, economia de custos de computação e tolerância a falhas.

```text
 [ Fontes Web ] 
  ├── Parquet (NYC TLC) ──( Python Requests / Stream 1MB )──> [ Camada Bronze ] ──> Dados Brutos Imutáveis (Nativos)
  └── CSV (Fornecedor)  ──( Escrita Atômica via .tmp   )──> 
                                                                   │
                                                           ( PySpark Session )
                                                                   ▼
                                                            [ Camada Silver ] ──> Schema Rígido, Limpeza e Paralelismo
                                                                   │
                                                                   ▼
                                                            [ Camada Gold ]   ──> Agregações e Prontidão para Analytics


```

## Estrutura 
```
├── data/
│   ├── 1_bronze/         # Dados brutos e imutáveis (.parquet e .csv originais)
│   ├── 2_silver/         # Dados limpos, tipados e enriquecidos pelo Spark
│   └── 3_gold/           # Visões agregadas e prontas para BI/Analytics
├── notebooks/
│   ├── 1_ingestao_bronze.ipynb   # Script Python Resiliente de Extração e Carga (EL)
│   └── 2_transformacao_silver.ipynb # Processamento pesado, Schema e Regras de Negócio (T)
├── README.md             # Documentação do projeto
└── requirements.txt      # Dependências do projeto (requests, pyspark, etc.)