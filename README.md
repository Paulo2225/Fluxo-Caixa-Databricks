# Fluxo de Caixa — Arquitetura Medallion no Databricks

Terceiro projeto da trilha de portfólio em dados: reconstrução do **Fluxo de Caixa** (já trabalhado nos projetos anteriores em SQL Server e Power BI) usando **Databricks**, com **PySpark**, **Delta Lake** e **Unity Catalog**, seguindo a arquitetura **Medallion (Bronze → Silver → Gold)**.

Projetos relacionados:
- [Fluxo de Caixa — Power BI (Projeto Base)](https://github.com/Paulo2225/Projeto-Fluxo-Caixa)
- [Evolução do Fluxo de Caixa — SQL Server + Python + Power BI](https://github.com/Paulo2225/Evolu-o-do-Fluxo-de-Caixa)

## Cenário de negócio

A empresa controla seu fluxo de caixa em 3 contas bancárias (Itaú Unibanco, Bradesco, Inter), registrando entradas e saídas categorizadas por plano de contas. O objetivo do projeto é estruturar esse fluxo em um **lakehouse**, permitindo rastreabilidade do dado bruto até o indicador final, e calculando o **saldo final por banco** a partir do saldo inicial e da movimentação do período.

## Arquitetura

```
Excel (Bancos, Movimentos, PlanoContas, SaldoAnterior)
        │
        ▼
   BRONZE  →  leitura bruta dos arquivos, sem tratamento, gravada como tabela Delta
        │
        ▼
   SILVER  →  limpeza (duplicatas, texto, tipos de data), cruzamento de Movimentos
              com Bancos para trazer Banco_ID confiável
        │
        ▼
    GOLD   →  agregações prontas para consumo: resumo mensal por banco/tipo,
              e saldo final por banco (saldo inicial + movimentação)
        │
        ▼
   Power BI (conexão direta às tabelas Gold via Databricks SQL Warehouse)
```

Cada camada tem um notebook próprio, refletindo a separação de responsabilidade do modelo Medallion:

| Notebook | Camada | O que faz |
|---|---|---|
| `fluxo-caixa-bronze` | Bronze | Lê os 4 arquivos Excel (pandas + openpyxl) e grava como tabelas Delta, sem transformação |
| `fluxo-caixa-silver` | Silver | Remove duplicatas, padroniza texto, corrige tipo de data, e cruza `Movimentos` com `Bancos` (join) para trazer `Banco_ID` |
| `fluxo-caixa-gold` | Gold | Agrega os dados por Ano/Mês/Banco/Tipo, e calcula o saldo final por banco |

## Estrutura de dados (Unity Catalog)

`catalog: workspace` → `schema: fluxo_caixa`

**Bronze**
- `bronze_bancos` — dimensão de bancos
- `bronze_movimentos` — fato de movimentações (entradas/saídas)
- `bronze_planocontas` — dimensão de plano de contas
- `bronze_saldoanterior` — saldo inicial por banco

**Silver**
- `silver_movimentos` — movimentações limpas, com `Banco_ID` resolvido via join
- `silver_planocontas` — plano de contas com texto padronizado
- `silver_saldoanterior` — saldo inicial validado (sem nulo em `Banco_ID`/`Valor`)

**Gold**
- `gold_resumo_mensal` — total de entradas/saídas por Ano, Mês, Banco e Tipo
- `gold_saldo_final` — saldo inicial, total movimentado e saldo final por banco

## Ferramentas utilizadas

- **Databricks Free Edition** (compute serverless, Unity Catalog)
- **PySpark** — leitura, transformação e agregação dos dados
- **pandas + openpyxl** — leitura dos arquivos Excel de origem
- **Delta Lake** — formato de armazenamento das tabelas em todas as camadas
- **Power BI** — consumo das tabelas Gold via conector nativo Databricks

## Decisões técnicas e limitações

- O **Free Edition** do Databricks não possui Jobs/Workflows (orquestração automatizada) nem cluster clássico configurável — todo o processamento roda em compute serverless, executado manualmente por notebook. Em um ambiente de produção, esse pipeline seria orquestrado via **Databricks Workflows**, com agendamento e dependência entre tasks (Bronze → Silver → Gold).
- O cruzamento de `Movimentos` com `Bancos` (join pelo nome do banco) foi validado sem nenhuma linha órfã (`Banco_ID` nulo), confirmando consistência entre as duas fontes.

## Próximos passos possíveis

- Orquestração via Databricks Workflows (ambiente pago)
- Testes automatizados de qualidade de dados (ex: `Great Expectations` ou `dbt tests`)
- Streaming incremental (Auto Loader) para simular chegada contínua de novas transações

---

**Autor:** Paulo Miguel Borges da Costa
[GitHub](https://github.com/Paulo2225) · [LinkedIn](https://www.linkedin.com/in/paulo-miguel-0458b0278)
