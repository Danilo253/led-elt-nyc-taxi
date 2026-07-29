# 📊 Relatório Técnico: Pipeline de Ingestão e Tratamento de Dados (NYC Yellow Taxi — Jan/2024)

## 1. Resumo Executivo
Este relatório documenta o pipeline de dados desenvolvido para ingestão, saneamento e consolidação das camadas **Bronze (Raw)** e **Silver (Cleansed)** da base de dados dos táxis amarelos de Nova York (*NYC Yellow Taxi* - Janeiro de 2024), além de sua tabela de apoio *Taxi Zone Lookup*.

O objetivo central foi estruturar um ecossistema **ELT resiliente e idempotente**, tratando inconsistências financeiras, temporais e geográficas sem perda acidental de dados legítimos, aplicando regras da *NYC Taxi and Limousine Commission (TLC)*.

---

## 2. Camada Bronze: Ingestão de Dados (ELT Resiliente)

### 2.1. Premissas Arquiteturais
* **ELT Legítimo & Imutabilidade:** Os dados foram baixados e mantidos em seu formato bruto e original no Data Lake, sem aplicação de esquemas rigorosos ou conversões no momento do download.
* **Eficiência de Recursos:** Optou-se por Python nativo (`requests` e `pathlib`) na ingestão, eliminando a sobrecarga (*overhead*) de cluster Spark para simples download e prevenindo inferências errôneas de esquema (`inferSchema`).
* **Mecanismo de Cache:** Checagem prévia de existência local para evitar requisições redundantes e otimizar banda de rede.

### 2.2. Resiliência Operacional
* **Download em Chunks (1 MB):** Processamento streaming em blocos contínuos de 1 MB, mantendo o consumo de memória RAM baixo e constante.
* **Escrita Atômica (`.tmp`):** Gravado em arquivo temporário antes da validação final. Em caso de queda de conexão ou falha HTTP (Timeout 30s), o parcial é destruído (`.unlink()`), impedindo a corrupção do Data Lake.
* **Status Final:**
  * Base Principal: `yellow_tripdata_2024-01.parquet` recuperada com sucesso.
  * Tabela Auxiliar: `taxi_zone_lookup.csv` integrada.

---

## 3. Camada Silver: Engenharia e Tratamento de Dados

A base inicial contou com **2.964.624 registros**. O processo de higienização seguiu critérios técnicos rigorosos para garantir a integridade analítica na camada Silver.

### 3.1. Enriquecimento e Colunas Derivadas
Foram criadas três métricas essenciais para validações e consumo analítico:
1. **`trip_duration_min`**: Tempo total da corrida em minutos (`dropoff_datetime` − `pickup_datetime`).
2. **`average_speed_mph`**: Velocidade média implícita ($\text{distância} / \text{horas}$). Protegida com `NULL` caso a duração fosse $\le 0$.
3. **`cost_per_mile`**: Custo por milha ($\text{fare\_amount} / \text{trip\_distance}$). Protegida com `NULL` se a distância fosse igual a 0.

---

### 3.2. Tratamento de Valores Nulos e Regra de Negócio *Flex Fare*
Identificou-se que **100% dos valores nulos** nas colunas `RatecodeID`, `passenger_count`, `store_and_fwd_flag`, `congestion_surcharge` e `Airport_fee` concentravam-se em corridas com método de pagamento **Flex Fare (`payment_type = 0`)** (140.162 linhas).

* **Contexto de Negócio:** O *Flex Fare* é um modelo da TLC que estabelece preço fixo antecipado antes da viagem, não acionando o ciclo do taxímetro tradicional (onde `RatecodeID` e retenção de sinal `store_and_fwd_flag` operam).
* **Tratamento:**
  * **Variáveis Categóricas:** Preenchidas diretamente com a string `"invalido"`.
  * **Variáveis Numéricas:** Mantidas como `NULL` para não distorcer agregações matemáticas, acompanhadas de colunas *flag* de status (`passenger_count_status`, `congestion_surcharge_status`, `Airport_fee_status`) rotuladas como `"invalido"`.

---

### 3.3. Higienização Temporal e Filtros de Domínio
1. **Domínio Temporal:** Removidos **18 registros** fora da janela de Janeiro/2024.
2. **Violação Lógica de Horário:** Removidos **56 registros** onde o desembarque era anterior ao embarque (`dropoff < pickup`).
3. **Jornada Extrema (Outliers de Duração):** Segundo diretrizes da TLC, motoristas não podem exceder 10 horas de passageiros a bordo em um período de 24h. Foram excluídas **1.651 corridas** com duração superior a 10 horas ($> 600\text{ min}$).

---

### 3.4. Reconciliação Financeira e Valores Anômalos
* **Validação da Equação Financeira:**
  
  $$\text{calculated_total} = \text{fare_amount} + \text{extra} + \text{mta_tax} + \text{tip_amount} + \text{tolls_amount} + \text{improvement_surcharge} + \text{congestion_surcharge} + \text{Airport_fee}$$
  
  * Reconciliação automatizada de taxas não computadas na origem para Flex Fare/MTA/Aeroportos. Registros com divergência irredutível ($|\Delta| > 0{,}02$) foram removidos (**5.196 linhas**).
* **Filtros Monetários de Domínio:**
  * Exclusão de valores fora de domínio legal (`mta_tax`, `congestion_surcharge`) — **9 linhas**.
  * Taxas descontinuadas (`improvement_surcharge = 0.30`) — **526 linhas**.
  * Tarifas com valor discrepante sem apoio tarifário (`fare_amount > 500`) — **45 linhas**.
  * Gorjetas anômalas (`tip_amount > 100` e $> 3\times$ o valor da tarifa) — **18 linhas**.
  * Exclusão de combinações inconsistentes (total positivo contendo componentes de tarifa negativos) — **2.064 linhas**.

---

### 3.5. Reconciliação de Estornos e Negativos
Identificação e tratamento do ciclo de estornos no taxímetro:
* **Pares de Estorno:** Localização de espelhamentos perfeitos (mesmo horário, locais, passageiros e valores opostos). Foram removidas **30.570 metades negativas com par positivo**, preservando o lançamento real original.
* **Estornos Órfãos:** Removidos registros negativos sem correspondência na base (**4.826 linhas**).
* **Total Removido no Bloco Financeiro:** **35.396 linhas**.

---

### 3.6. Consistência de Deslocamento e Distâncias Zeras
* **Velocidades Irrealistas:** Exclusão de corridas com velocidade média implícita superior a **80 mph (~128 km/h)** no tráfego urbano — **1.854 linhas**.
* **Distância Zerada (`trip_distance == 0`):**
  * **Legítimas:** Classificadas como `"invalido"` na coluna `status_trip_distance` quando respaldadas por regimes tarifários (*Flex Fare*, *Tabela Fixa JFK/Manhattan*, *Tarifa Negociada*) ou se `PULocationID != DOLocationID` (prova de deslocamento).
  * **Ilegítimas:** Sem evidência de que a corrida ocorreu (`PULocationID == DOLocationID` e tarifa normal) — **12.565 linhas removidas**.

---

### 3.7. Validação da Capacidade de Passageiros
* Removidos **47 registros** com `passenger_count >= 7`, por exceder a capacidade máxima homologada para veículos das frotas de táxi amarelo.
* Registros com `passenger_count == 0` foram categorizados como `"invalido"` na coluna de status, mantendo as linhas para fins de auditoria monetária.

---

## 4. Balanço Final da Camada Silver

| Métrica / Etapa | Volume de Linhas | Impacto (%) |
| :--- | :---: | :---: |
| **Volumetria Bruta Inicial (Bronze)** | **2.964.624** | **100,00%** |
| (*) Registros fora de Jan/2024 | -18 | -0,0006% |
| (*) Dropoff anterior ao Pickup | -56 | -0,0019% |
| (*) Duração anômala (> 10h) | -1.651 | -0,0557% |
| (*) Inconsistências de Reconciliação Monetária | -5.196 | -0,1753% |
| (*) Taxas fora do domínio legal / Tarifas > $500 | -598 | -0,0202% |
| (*) Inconsistências de Sinais Monetários / Gorjetas | -2.082 | -0,0702% |
| (*) Estornos e Registros Negativos | -35.396 | -1,1939% |
| (*) Velocidade irrealista (> 80 mph) | -1.854 | -0,0625% |
| (*) Distância zero sem evidência de corrida | -12.565 | -0,4238% |
| (*) Capacidade de passageiros >=7 | -47 | -0,0016% |
| **Volumetria Final Processada (Silver)** | **2.905.179** | **98,00%** |

---

## 5. Conclusão e Entrega da Camada Silver

O pipeline consolidou um purgo total de **59.445 registros inconsistentes** (~2% do volume total), entregando uma base limpa com **2.905.179 linhas e 27 colunas** otimizadas. 

A tabela tratada foi exportada no formato otimizado Apache Parquet para o diretório de destino:
`data/2_Silver/yellow_trips_silver/silver.parquet`

A base Silver encontra-se pronta e confiável para alimentar visões analíticas, modelos preditivos e a **Camada Gold**.