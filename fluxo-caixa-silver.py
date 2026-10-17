# Databricks notebook source
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

display(df_mov_silver)

# COMMAND ----------

display(df_mov_silver.filter(col("Banco_ID").isNull()))

# COMMAND ----------

from pyspark.sql.functions import col, trim, initcap

df_pc = spark.table("workspace.fluxo_caixa.bronze_planocontas")

df_pc_silver = (
    df_pc
    .dropDuplicates()
    .withColumn("Subgrupo", trim(initcap(col("Subgrupo"))))
    .withColumn("Conta", trim(initcap(col("Conta"))))
    .filter(col("Conta_ID").isNotNull())
)

df_pc_silver.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.fluxo_caixa.silver_planocontas")

display(df_pc_silver)

# COMMAND ----------

from pyspark.sql.functions import col

df_sa = spark.table("workspace.fluxo_caixa.bronze_saldoanterior")

df_sa_silver = (
    df_sa
    .dropDuplicates()
    .filter(col("Banco_ID").isNotNull())
    .filter(col("Valor").isNotNull())
)

df_sa_silver.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.fluxo_caixa.silver_saldoanterior")

display(df_sa_silver)