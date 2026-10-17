# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import pandas as pd

arquivos = ["Bancos", "Movimentos", "PlanoContas", "SaldoAnterior"]

for nome in arquivos:
    caminho = f"/Volumes/workspace/fluxo_caixa/bronze_raw/{nome}.xlsx"
    pdf = pd.read_excel(caminho)
    df = spark.createDataFrame(pdf)
    df.write.format("delta").mode("overwrite") \
        .saveAsTable(f"workspace.fluxo_caixa.bronze_{nome.lower()}")

print("Tabelas Bronze criadas.")

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.fluxo_caixa"))

# COMMAND ----------

display(spark.table("workspace.fluxo_caixa.bronze_bancos").limit(5))
display(spark.table("workspace.fluxo_caixa.bronze_movimentos").limit(5))
display(spark.table("workspace.fluxo_caixa.bronze_planocontas").limit(5))
display(spark.table("workspace.fluxo_caixa.bronze_saldoanterior").limit(5))