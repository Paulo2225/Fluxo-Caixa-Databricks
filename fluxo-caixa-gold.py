# Databricks notebook source
from pyspark.sql.functions import sum as _sum, month, year, col, round as _round

df_mov = spark.table("workspace.fluxo_caixa.silver_movimentos")

df_gold = (
    df_mov
    .groupBy(
        year("Data").alias("Ano"),
        month("Data").alias("Mes"),
        "Banco_ID",
        "Banco",
        "Tipo"
    )
    .agg(_round(_sum("Valor"), 2).alias("Total"))
)

df_gold.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.fluxo_caixa.gold_resumo_mensal")

display(df_gold)

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, when, col, round as _round

df_mov = spark.table("workspace.fluxo_caixa.silver_movimentos")
df_sa = spark.table("workspace.fluxo_caixa.silver_saldoanterior")

df_saldo_por_banco = (
    df_mov
    .groupBy("Banco_ID", "Banco")
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

display(df_saldo_final)