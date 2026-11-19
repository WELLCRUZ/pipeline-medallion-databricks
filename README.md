# 🥇 Pipeline Medallion no Azure Databricks (Bronze → Silver → Gold)

Pipeline de dados ETL/ELT construído no **Azure Databricks**, seguindo a **Arquitetura Medallion**, com governança via **Unity Catalog** e armazenamento em **Delta Lake**. O projeto leva dados brutos até uma camada analítica pronta para consumo em BI.

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD4?style=flat&logo=delta&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-336791?style=flat&logo=postgresql&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=flat&logo=powerbi&logoColor=black)

> ⚠️ Projeto de estudo e portfólio. Todos os dados são **fictícios ou públicos**, sem qualquer informação de empresas reais.

---

## 🎯 Problema

Dados operacionais chegam de forma bruta, com inconsistências (duplicidades, nulos, tipos incorretos e formatos diferentes), o que dificulta a criação de indicadores confiáveis. O objetivo é organizar esse fluxo em camadas, de modo que cada etapa aumente a qualidade e a confiabilidade dos dados.

## 🏗️ Arquitetura

```
 Fonte (CSV/JSON)
        │
        ▼
 ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
 │    BRONZE    │ ──▶ │    SILVER    │ ──▶ │     GOLD     │ ──▶  Power BI
 │ Dado bruto   │     │ Limpo e      │     │ Agregado e   │
 │ (como chegou)│     │ padronizado  │     │ regras de    │
 │              │     │              │     │ negócio      │
 └──────────────┘     └──────────────┘     └──────────────┘
        Delta Lake  ·  Unity Catalog  ·  Azure
```

| Camada | Objetivo | O que acontece |
|---|---|---|
| **Bronze** | Preservar o dado original | Ingestão sem alteração, com data de carga |
| **Silver** | Qualidade e padronização | Remoção de duplicados, tratamento de nulos, ajuste de tipos |
| **Gold** | Valor para o negócio | Agregações, CTEs e regras de negócio para indicadores |

## 🧰 Tecnologias e conceitos aplicados

- **Azure Databricks** como plataforma de processamento
- **Unity Catalog:** catálogos, schemas, **External Locations** e volumes (externos e gerenciados)
- **Delta Lake:** tabelas Delta criadas via SQL com External Location
- **Arquitetura Medallion** com validação e dependências entre tasks
- **Jobs e Pipelines:** Schedules & Triggers, notificações e gerenciamento de falhas
- **SQL:** CTEs, criação de tabelas, filtros e transformações

## 🔄 Passo a passo

1. **Estruturação do ambiente:** criação de container, catálogo, schemas e volumes no Unity Catalog
2. **External Location:** configuração do acesso ao armazenamento no Azure
3. **Bronze:** ingestão dos arquivos de origem em tabelas Delta, sem transformação
4. **Silver:** limpeza, deduplicação, padronização de tipos e validações de qualidade
5. **Gold:** construção de tabelas analíticas com CTEs e regras de negócio
6. **Orquestração:** criação de Job com tasks dependentes (Bronze → Silver → Gold), agendamento (Schedule/Trigger) e notificação em caso de falha
7. **Consumo:** conexão da camada Gold ao Power BI para visualização

## 💻 Código SQL por camada

> Os nomes de catálogo, schema, container e colunas são exemplos. Ajuste para os do seu projeto.

### 0. Setup do ambiente (Unity Catalog)

```sql
-- Catálogo e schemas das três camadas
CREATE CATALOG IF NOT EXISTS meu_catalogo;
CREATE SCHEMA IF NOT EXISTS meu_catalogo.bronze;
CREATE SCHEMA IF NOT EXISTS meu_catalogo.silver;
CREATE SCHEMA IF NOT EXISTS meu_catalogo.gold;

-- External Location apontando para o container no Azure
CREATE EXTERNAL LOCATION IF NOT EXISTS meu_local_externo
URL 'abfss://meu-container@minhaconta.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL minha_credencial);

-- Volume para os arquivos de origem (landing)
CREATE VOLUME IF NOT EXISTS meu_catalogo.bronze.landing;
```

### 1. Bronze: ingestão do dado bruto

Tudo entra como texto, sem transformação, com data de carga e arquivo de origem para rastreabilidade.

```sql
CREATE TABLE IF NOT EXISTS meu_catalogo.bronze.vendas (
  id_pedido      STRING,
  data_pedido    STRING,
  categoria      STRING,
  valor          STRING,
  data_carga     TIMESTAMP,
  arquivo_origem STRING
)
USING DELTA;

COPY INTO meu_catalogo.bronze.vendas
FROM (
  SELECT
    id_pedido,
    data_pedido,
    categoria,
    valor,
    current_timestamp()   AS data_carga,
    _metadata.file_path   AS arquivo_origem
  FROM '/Volumes/meu_catalogo/bronze/landing/vendas/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false');
```

O `COPY INTO` é idempotente: arquivos já carregados não são reprocessados.

### 2. Silver: limpeza e padronização

```sql
CREATE OR REPLACE TABLE meu_catalogo.silver.vendas AS
WITH base AS (
  SELECT
    TRY_CAST(id_pedido AS BIGINT)         AS id_pedido,
    TRY_CAST(data_pedido AS DATE)         AS data_pedido,
    UPPER(TRIM(categoria))                AS categoria,
    TRY_CAST(valor AS DECIMAL(10,2))      AS valor,
    data_carga,
    ROW_NUMBER() OVER (
      PARTITION BY id_pedido
      ORDER BY data_carga DESC
    ) AS rn
  FROM meu_catalogo.bronze.vendas
  WHERE id_pedido IS NOT NULL
)
SELECT id_pedido, data_pedido, categoria, valor, data_carga
FROM base
WHERE rn = 1                 -- remove duplicados, mantendo a carga mais recente
  AND data_pedido IS NOT NULL
  AND valor IS NOT NULL
  AND valor >= 0;
```

O `TRY_CAST` devolve `NULL` em vez de quebrar o pipeline quando o valor é inválido, e esses registros são descartados pelos filtros.

### 3. Validação de qualidade (entre Silver e Gold)

Esta consulta pode ser uma task do Job: se devolver linhas, o pipeline deve falhar e notificar.

```sql
-- Retorna linhas apenas se houver problema
SELECT 'ids duplicados' AS problema, COUNT(*) AS qtd
FROM (
  SELECT id_pedido
  FROM meu_catalogo.silver.vendas
  GROUP BY id_pedido
  HAVING COUNT(*) > 1
)
HAVING COUNT(*) > 0

UNION ALL

SELECT 'valores negativos', COUNT(*)
FROM meu_catalogo.silver.vendas
WHERE valor < 0
HAVING COUNT(*) > 0

UNION ALL

SELECT 'tabela vazia', 1
FROM (SELECT COUNT(*) AS total FROM meu_catalogo.silver.vendas)
WHERE total = 0;
```

### 4. Gold: indicadores para BI

```sql
CREATE OR REPLACE TABLE meu_catalogo.gold.vendas_mensais AS
WITH mensal AS (
  SELECT
    DATE_TRUNC('month', data_pedido) AS mes,
    categoria,
    COUNT(*)                         AS qtd_pedidos,
    SUM(valor)                       AS receita,
    ROUND(AVG(valor), 2)             AS ticket_medio
  FROM meu_catalogo.silver.vendas
  GROUP BY DATE_TRUNC('month', data_pedido), categoria
),
com_variacao AS (
  SELECT
    *,
    LAG(receita) OVER (PARTITION BY categoria ORDER BY mes) AS receita_mes_anterior
  FROM mensal
)
SELECT
  mes,
  categoria,
  qtd_pedidos,
  receita,
  ticket_medio,
  receita_mes_anterior,
  ROUND(
    100.0 * (receita - receita_mes_anterior)
    / NULLIF(receita_mes_anterior, 0), 2
  ) AS variacao_receita_pct
FROM com_variacao;
```

Esta tabela é a que o Power BI consome: receita, ticket médio e variação mensal por categoria.
