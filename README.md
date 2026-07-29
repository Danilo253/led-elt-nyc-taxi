# PS - LED 2026 · Grupo 18
# 🚖 NYC Taxi Data Pipeline: Arquitetura Lakehouse com ELT Resiliente

[![Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![GitHub](https://img.shields.io/badge/DataOps-Resilience-green?style=for-the-badge)]()

Este projeto implementa um pipeline de dados no padrão **ELT (Extract, Load, Transform)** sobre a arquitetura **Medalhão (Bronze → Silver → Gold)**, culminando em um **Modelo Dimensional (Star Schema)** pronto para análise OLAP. O objetivo é extrair os dados históricos de corridas de táxi amarelo de Nova York (**NYC TLC — Yellow Taxi, Janeiro/2024**), garantir resiliência de rede na ingestão, aplicar tratamento rigoroso de qualidade e disponibilizar um Data Warehouse analítico.

> **Fonte dos dados:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — `yellow_tripdata_2024-01.parquet` (2.964.624 registros brutos) + `taxi_zone_lookup.csv` (dicionário de zonas).

---

## 🏗️ Desenho da Arquitetura

O pipeline segue as premissas modernas de Engenharia de Dados (DataOps): separação estrita de responsabilidades por camada, economia de custo computacional na ingestão e tolerância a falhas.

```text
 [ Fontes Web ]
  ├── Parquet (NYC TLC) ──( Python Requests / Stream 1MB )──> [ Camada Bronze ] ──> Dados Brutos Imutáveis (Nativos)
  └── CSV (Taxi Zones)  ──( Escrita Atômica via .tmp     )──>          │
                                                                       │  ( EDA — investigação de qualidade )
                                                                       ▼
                                                              ( PySpark Session )
                                                                       ▼
                                                              [ Camada Silver ] ──> Schema tipado, limpeza,
                                                                       │            colunas derivadas e flags de qualidade
                                                                       ▼
                                                              [ Camada Gold ]   ──> Star Schema (Kimball):
                                                                                    1 Fato + 3 Dimensões · OLAP-ready
```

**Princípio de qualidade adotado:** sempre que a decisão de negócio é ambígua, preferimos **colunas de status (flag)** — por exemplo `passenger_count_status`, `average_speed_mph_status` — em vez de correção destrutiva no valor original. Isso preserva o tipo numérico das colunas para agregações e mantém a rastreabilidade de auditoria até a camada Gold.

---

## 🔄 Etapas do Pipeline

### 0. EDA — Análise Exploratória e Investigação de Qualidade
`01_eda_analysis_team18.ipynb`

Investigação profunda que fundamenta todas as regras aplicadas na Silver. Cobre a descrição de colunas via documentação oficial, tipos, volume, cardinalidade, valores nulos, distribuição de categóricas/numéricas e uma análise sistemática de outliers e anomalias:
- **Temporais:** registros fora de Jan/2024, `dropoff` anterior ao `pickup`, durações acima de 10h.
- **Monetárias:** validação matemática da equação do `total_amount`, valores anômalos por coluna (`extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, etc.), registros negativos e sua ligação com estornos/reembolsos e com padrões por `VendorID`.
- **Distância:** outliers via velocidade implícita e o fenômeno `trip_distance == 0` segmentado por regime tarifário (Flex Fare, JFK↔Manhattan, Negotiated fare).

### 1. Bronze — Extract & Load (EL puro)
`02_extract_and_load_bronze.ipynb`

Ingestão em Python nativo (`requests` + `pathlib`), **sem Spark**, para evitar overhead e o risco de `inferSchema` sobre dado bruto:
- **Streaming em chunks de 1 MB** — consumo de RAM fixo, independente do tamanho do arquivo.
- **Escrita atômica via `.tmp`** — o `.parquet` oficial só existe quando 100% íntegro; falhas de rede deletam o parcial (`.unlink()`).
- **Cache idempotente** — não rebaixa arquivos já presentes no disco.
- Tratamento de HTTP (Status 200) e `timeout=30`.

### 2. Silver — Transform (limpeza e enriquecimento)
`03_Trasformacao_load_silver.ipynb`

Processamento distribuído em **PySpark**, com schema rígido e as regras validadas na EDA:
- **Colunas derivadas:** `trip_duration_min`, `average_speed_mph` (protegida contra divisão por zero) e `cost_per_mile`.
- **Nulos do Flex Fare (`payment_type = 0`):** categóricas → rótulo `"invalido"`; numéricas → valor mantido como `NULL` + coluna `_status` separada.
- **Consistência temporal, monetária, de distância e de `passenger_count`**, conforme investigado na EDA.
- **Saída:** Parquet único em `data/2_Silver/yellow_trips_silver/silver.parquet`.

### 3. Gold — Modelagem Dimensional (Star Schema)
`04_Modelo_Dimensional_load_gold.ipynb`

Modelagem analítica segundo **Ralph Kimball**, com granularidade atômica (**1 linha = 1 corrida**) para preservar sinal e permitir *drill-down/roll-up* completo:

| Tabela | Tipo | Descrição |
|---|---|---|
| `fato_corridas` | Fato | Métricas físicas, de desempenho e financeiras + FKs e flags de auditoria |
| `dim_calendario` | Dimensão | Calendário de Jan/2024 (dia, semana, fim de semana, trimestre) |
| `dim_pagamento` | Dimensão | Descrição dos códigos de `payment_type` |
| `dim_tarifa` | Dimensão | Descrição dos códigos de `RatecodeID` |

Inclui **consultas OLAP** (PySpark DataFrame e SQL nativo) e visualizações analíticas (horário de pico × velocidade, dia útil × fim de semana por tarifa, heatmap de eficiência $/minuto).

---

## 📂 Estrutura do Projeto

```text
├── data/
│   ├── 1_bronze/
│   │   └── yellow_trips/                    # Dados brutos imutáveis (.parquet + taxi_zone_lookup.csv)
│   ├── 2_Silver/
│   │   └── yellow_trips_silver/             # Dado limpo, tipado e enriquecido (silver.parquet)
│   └── 3_gold/                              # Star Schema
│       ├── fato_corridas/
│       ├── dim_calendario/
│       ├── dim_pagamento/
│       └── dim_tarifa/
├── notebooks/
│   ├── 01_eda_analysis_team18.ipynb         # EDA e investigação de qualidade
│   ├── 02_extract_and_load_bronze.ipynb     # Extração e Carga resiliente (EL)
│   ├── 03_Trasformacao_load_silver.ipynb    # Transformação, schema e regras de negócio (T)
│   └── 04_Modelo_Dimensional_load_gold.ipynb # Star Schema e consultas OLAP
├── README.md                                # Documentação do projeto
└── requirements.txt                         # Dependências (pyspark, requests, pandas, pyarrow, matplotlib, seaborn)
```

---

## ⚙️ Stack & Dependências

- **Processamento distribuído:** Apache Spark / PySpark
- **Ingestão:** Python nativo (`requests`, `pathlib`)
- **Escrita Parquet:** Pandas + PyArrow
- **Visualização:** Matplotlib · Seaborn

```bash
pip install -r requirements.txt
```

---

## ▶️ Como Executar

Rode os notebooks na ordem numérica:

1. `02_extract_and_load_bronze.ipynb` — baixa o Parquet e o CSV para a camada Bronze.
2. `01_eda_analysis_team18.ipynb` — (opcional/investigativo) fundamenta as regras de qualidade.
3. `03_Trasformacao_load_silver.ipynb` — gera a camada Silver.
4. `04_Modelo_Dimensional_load_gold.ipynb` — constrói o Star Schema e executa as análises OLAP.

> **Nota:** o notebook da camada Gold aponta para o caminho da Silver no `path_silver`. Ajuste-o conforme seu ambiente (execução local vs. Google Colab / Drive).

---

**Grupo 18 — Programa de Formação em Engenharia de Dados (LED 2026).**
