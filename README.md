# Fluxo de Caixa — Arquitetura Medallion no Databricks

## 1. Apresentação do Projeto

### 1.1 O que é este projeto

Este projeto é uma nova evolução do controle de fluxo de caixa já trabalhado nos dois projetos anteriores — o primeiro em Power BI puro, o segundo estruturado em SQL Server com camadas staging, dimensional e fato.

Nesta terceira versão, os mesmos dados (bancos, movimentações, plano de contas e saldo anterior) foram reorganizados utilizando o **Databricks**, aplicando a arquitetura **Medallion (Bronze, Silver e Gold)** com **PySpark** e **Delta Lake**, e conectando o resultado final diretamente ao Power BI.

A proposta não foi mudar o problema de negócio, e sim aprender na prática como um mesmo conjunto de dados pode ser reestruturado em uma plataforma de lakehouse, diferente da abordagem relacional tradicional usada no projeto anterior.

---

### 1.2 Objetivos do Projeto

- estruturar os dados em camadas Bronze, Silver e Gold, seguindo a arquitetura Medallion
- utilizar PySpark para leitura, limpeza e agregação dos dados
- utilizar Delta Lake como formato de armazenamento em todas as camadas
- compreender o papel do Unity Catalog na organização de catálogos, schemas e volumes
- conectar os dados finais (camada Gold) diretamente ao Power BI
- entender, na prática, a diferença entre um fluxo relacional (SQL Server) e um fluxo em lakehouse (Databricks)

---

### 1.3 Escopo do Projeto

Dentro desse escopo, foram realizadas as seguintes etapas:

- criação do ambiente no Databricks (Free Edition), com catálogo, schema e volume no Unity Catalog
- leitura dos arquivos Excel de origem (`Bancos`, `Movimentos`, `PlanoContas`, `SaldoAnterior`) e gravação como tabelas Delta na camada Bronze
- limpeza e padronização dos dados na camada Silver, incluindo o cruzamento entre `Movimentos` e `Bancos`
- agregação dos dados na camada Gold, com resumo mensal por banco/tipo e cálculo de saldo final por banco
- conexão das tabelas Gold ao Power BI via Databricks SQL Warehouse

---

## 2. Justificativa do Projeto

### 2.1 Problema Identificado

No projeto anterior, os dados já estavam organizados em um banco relacional (SQL Server), com staging, dimensões e fato. Essa estrutura funcionava bem, mas o processamento inteiro dependia de um servidor de banco de dados tradicional, e as transformações ficavam concentradas em SQL.

Surgiu o interesse em entender como esse mesmo fluxo poderia ser reconstruído em uma plataforma de **lakehouse**, onde o processamento é feito com Spark e o armazenamento é feito em Delta Lake — uma abordagem cada vez mais usada em ambientes de dados em nuvem, diferente do modelo relacional tradicional.

### 2.2 Solução Proposta

A solução foi reconstruir o pipeline dentro do Databricks, dividindo o processamento em três camadas:

- **Bronze** — os dados chegam exatamente como estão nos arquivos Excel, sem nenhum tratamento, apenas convertidos para tabela Delta
- **Silver** — os dados são limpos (duplicatas removidas, texto padronizado, tipos corrigidos) e relacionados entre si
- **Gold** — os dados são agregados nas métricas finais, prontas para consumo em ferramentas de BI

Com essa abordagem, cada camada tem uma responsabilidade clara, e o Power BI passa a consumir apenas a camada Gold, já pronta — sem precisar repetir tratamento de dado dentro do próprio Power Query.

---

## 3. Arquitetura da Solução

```
Excel (Bancos, Movimentos, PlanoContas, SaldoAnterior)
        │
        ▼
   BRONZE   →  leitura bruta dos arquivos, sem tratamento, gravada como tabela Delta
        │
        ▼
   SILVER   →  remoção de duplicatas, padronização de texto, correção de tipo de data,
               e cruzamento de Movimentos com Bancos para trazer o Banco_ID
        │
        ▼
    GOLD    →  agregação por Ano/Mês/Banco/Tipo, e cálculo do saldo final por banco
        │
        ▼
   Power BI (conexão direta às tabelas Gold via Databricks SQL Warehouse)
```

Cada camada foi implementada em um notebook separado, mantendo a mesma lógica de separação de responsabilidade adotada nas tabelas staging/dimensional/fato do projeto anterior:

| Notebook | Camada |
|---|---|
| `fluxo-caixa-bronze` | Bronze |
| `fluxo-caixa-silver` | Silver |
| `fluxo-caixa-gold` | Gold |

---

## 4. Documentação Técnica

### 4.1 Ambiente e Estrutura de Dados (Unity Catalog)

**Explicação do que foi feito:** Antes de processar qualquer dado, foi criada a estrutura de organização no Unity Catalog do Databricks: um catálogo (`workspace`), um schema (`fluxo_caixa`) e um volume (`bronze_raw`) para receber os arquivos Excel originais.

**Explicação técnica:** No Databricks, o Unity Catalog organiza os dados em três níveis — catálogo, schema e tabela/volume — de forma parecida com banco/schema/tabela em um SQL Server tradicional. O volume funciona como uma pasta de arquivos brutos, distinta das tabelas Delta que armazenam dado estruturado.

**Motivo da decisão:** Seguir essa estrutura desde o início evita misturar arquivo bruto com tabela processada, e é o padrão atualmente recomendado pela própria Databricks para organização de dados.

---

### 4.2 Camada Bronze — Leitura dos Arquivos Excel

```python
import pandas as pd

arquivos = ["Bancos", "Movimentos", "PlanoContas", "SaldoAnterior"]

for nome in arquivos:
    caminho = f"/Volumes/workspace/fluxo_caixa/bronze_raw/{nome}.xlsx"
    pdf = pd.read_excel(caminho)
    df = spark.createDataFrame(pdf)
    df.write.format("delta").mode("overwrite") \
        .saveAsTable(f"workspace.fluxo_caixa.bronze_{nome.lower()}")
```

**Explicação do que foi feito:** Os quatro arquivos Excel de origem foram lidos e gravados como tabelas Delta na camada Bronze, sem nenhuma transformação — o objetivo dessa camada é manter o dado exatamente como veio da fonte, para rastreabilidade.

**Explicação técnica:** O Spark não lê arquivos `.xlsx` nativamente, por isso a leitura é feita com `pandas` (`pd.read_excel`), e o resultado é convertido para um DataFrame do Spark com `spark.createDataFrame`, que é então gravado como tabela Delta (`saveAsTable`). O laço `for` evita repetir o mesmo bloco de código para cada um dos quatro arquivos.

**Motivo da decisão:** Manter uma camada bruta, sem tratamento, permite voltar ao dado original a qualquer momento caso alguma transformação posterior precise ser revista ou auditada.

---

### 4.3 Camada Silver — Limpeza e Relacionamento

```python
from pyspark.sql.functions import col, to_date, trim, initcap

df_mov = spark.table("workspace.fluxo_caixa.bronze_movimentos")
df_bancos = spark.table("workspace.fluxo_caixa.bronze_bancos")

df_mov_silver = (
    df_mov
    .dropDuplicates()
    .withColumn("Tipo", trim(initcap(col("Tipo"))))
    .withColumn("Conta", trim(col("Conta")))
    .withColumn("Banco", trim(col("Banco")))
    .withColumn("Data", to_date(col("Data")))
    .filter(col("Valor").isNotNull())
    .join(df_bancos, on="Banco", how="left")
)

df_mov_silver.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.fluxo_caixa.silver_movimentos")
```

**Explicação do que foi feito:** A tabela de movimentações foi limpa (duplicatas removidas, texto padronizado, tipo de data corrigido) e cruzada com a tabela de bancos, trazendo o `Banco_ID` para dentro da tabela de movimentos — que originalmente só tinha o nome do banco em texto.

**Explicação técnica:** `dropDuplicates()` remove linhas repetidas; `trim` e `initcap` padronizam texto (removendo espaço em branco e uniformizando capitalização); `to_date` converte a coluna de data, que veio como texto/timestamp, para um tipo Data reconhecido pelo Spark. O `.join(df_bancos, on="Banco", how="left")` cruza cada linha de movimentação com a tabela de bancos pelo nome, trazendo o identificador numérico (`Banco_ID`). Foi feita uma checagem após o join (`filter(col("Banco_ID").isNull())`), confirmando que nenhuma linha ficou sem correspondência.

**Motivo da decisão:** Depender apenas do nome do banco como texto é frágil — qualquer inconsistência de digitação quebraria comparações futuras. Trazer o `Banco_ID` a partir de uma tabela de referência (dimensão) torna o relacionamento mais confiável, seguindo a mesma lógica de chave estrangeira usada no projeto anterior em SQL Server.

O mesmo padrão de limpeza (remoção de duplicatas, padronização de texto, checagem de nulos) foi aplicado também às tabelas `silver_planocontas` e `silver_saldoanterior`.

---

### 4.4 Camada Gold — Agregações Finais

```python
from pyspark.sql.functions import sum as _sum, month, year, col, round as _round

df_mov = spark.table("workspace.fluxo_caixa.silver_movimentos")

df_gold = (
    df_mov
    .groupBy(
        year("Data").alias("Ano"),
        month("Data").alias("Mes"),
        "Banco_ID", "Banco", "Tipo"
    )
    .agg(_round(_sum("Valor"), 2).alias("Total"))
)

df_gold.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.fluxo_caixa.gold_resumo_mensal")
```

```python
df_saldo_por_banco = (
    df_mov.groupBy("Banco_ID", "Banco")
    .agg(_round(_sum("Valor"), 2).alias("Total_Movimentado"))
)

df_saldo_final = (
    df_saldo_por_banco
    .join(df_sa, on="Banco_ID", how="left")
    .withColumn("Saldo_Inicial", col("Valor"))
    .withColumn("Saldo_Final", _round(col("Saldo_Inicial") + col("Total_Movimentado"), 2))
    .select("Banco_ID", "Banco", "Saldo_Inicial", "Total_Movimentado", "Saldo_Final")
)

df_saldo_final.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.fluxo_caixa.gold_saldo_final")
```

**Explicação do que foi feito:** Foram criadas duas tabelas Gold: um resumo mensal de entradas e saídas por banco (`gold_resumo_mensal`), e o cálculo do saldo final por banco (`gold_saldo_final`), somando o saldo inicial à movimentação total do período.

**Explicação técnica:** `groupBy` agrupa os dados pelas dimensões desejadas (ano, mês, banco, tipo), e `agg(_sum(...))` soma os valores dentro de cada grupo. A função `round()` foi aplicada para eliminar erro de ponto flutuante, comum em somas de valores decimais no Spark. No cálculo do saldo final, um segundo `join` relaciona o total movimentado por banco com o saldo inicial (`silver_saldoanterior`), e o saldo final é obtido somando as duas colunas.

**Motivo da decisão:** A camada Gold existe para entregar o dado já pronto para consumo — sem essa camada, o Power BI precisaria repetir a mesma lógica de agregação em DAX ou Power Query, duplicando trabalho e regra de negócio em dois lugares diferentes.

---

### 4.5 Conexão dos Dados no Power BI

**Explicação do que foi feito:** As tabelas Gold (`gold_resumo_mensal` e `gold_saldo_final`) foram conectadas diretamente ao Power BI Desktop, através do conector nativo do Databricks, autenticado via OAuth com a mesma conta usada no workspace.

**Explicação técnica:** A conexão foi feita informando o **Server Hostname** e o **HTTP Path** do SQL Warehouse do Databricks (disponíveis na aba "Connection details" do warehouse), usando o conector genérico "Databricks" do Power BI.

**Motivo da decisão:** Conectar direto às tabelas Gold, em vez de exportar CSV ou conectar em uma camada anterior, mantém o pipeline vivo — qualquer atualização nos dados de origem se reflete automaticamente até o Power BI, sem etapa manual intermediária.

---

### 4.6 Dicionário de Dados

**Camada Bronze**

| Tabela | Colunas |
|---|---|
| `bronze_bancos` | Banco_ID, Banco |
| `bronze_movimentos` | Tipo, Conta, Data, Banco, Valor |
| `bronze_planocontas` | Subgrupo_ID, Subgrupo, Conta_ID, Conta |
| `bronze_saldoanterior` | Banco_ID, Valor |

**Camada Silver**

| Tabela | Colunas |
|---|---|
| `silver_movimentos` | Banco, Tipo, Conta, Data, Valor, Banco_ID |
| `silver_planocontas` | Subgrupo_ID, Subgrupo, Conta_ID, Conta |
| `silver_saldoanterior` | Banco_ID, Valor |

**Camada Gold**

| Tabela | Colunas |
|---|---|
| `gold_resumo_mensal` | Ano, Mes, Banco_ID, Banco, Tipo, Total |
| `gold_saldo_final` | Banco_ID, Banco, Saldo_Inicial, Total_Movimentado, Saldo_Final |

---

## 5. Encerramento do Projeto

### Considerações Finais

Este projeto foi desenvolvido como uma nova evolução do fluxo de caixa trabalhado nos dois projetos anteriores, dessa vez migrando a estrutura de um banco relacional (SQL Server) para uma arquitetura de lakehouse no Databricks.

A principal mudança foi sair de tabelas staging/dimensional/fato em SQL para camadas Bronze/Silver/Gold em Delta Lake, processadas com PySpark. Isso permitiu entender, na prática, como um mesmo problema de negócio pode ser resolvido com ferramentas e paradigmas diferentes.

### Principais Aprendizados

- Estruturação de dados em arquitetura Medallion (Bronze, Silver, Gold)
- Uso de PySpark para leitura, limpeza e agregação de dados
- Uso do Delta Lake como formato de armazenamento
- Organização de dados via Unity Catalog (catálogo, schema, volume)
- Conexão de ferramentas de BI diretamente a um ambiente Databricks

### Conclusão

De forma geral, o projeto cumpriu seu objetivo de demonstrar, na prática, como reestruturar um fluxo de dados já conhecido dentro de uma plataforma de lakehouse. Essa experiência contribuiu para o entendimento de conceitos importantes de engenharia de dados moderna, complementando o aprendizado anterior em SQL Server e Power BI.

---

**Autor:** Paulo Miguel Borges da Costa
[GitHub](https://github.com/Paulo2225) · [LinkedIn](https://www.linkedin.com/in/paulo-miguel-0458b0278)
