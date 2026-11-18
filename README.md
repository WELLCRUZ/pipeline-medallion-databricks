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

## 💻 Exemplo de código (camada Silver)

> Ajuste os nomes de catálogo, schema e colunas para os do seu projeto.

```sql
CREATE OR REPLACE TABLE meu_catalogo.silver.vendas AS
WITH base AS (
  SELECT
