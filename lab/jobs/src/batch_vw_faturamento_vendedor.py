from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, round, sha2

# Inicializa Spark
spark = SparkSession.builder \
    .appName("VendasPorVendedorAnonimo") \
    .getOrCreate()

# Caminhos
input_path = "/data/vendas"
output_path = "/output/vendas_por_vendedor_anon"

# Leitura dos dados
df_vendas = spark.read.parquet(input_path)

# Garantir tipos corretos
df_vendas = df_vendas \
    .withColumn("id_vendedor", col("id_vendedor").cast("string")) \
    .withColumn("total", col("total").cast("decimal(10,2)"))

# Anonimização do identificador
df_vendas = df_vendas.withColumn("hash_id_vendedor", sha2(col("id_vendedor"), 256))

# Agregação por vendedor anonimizado
vendas_por_vendedor = df_vendas.groupBy("hash_id_vendedor").agg(
    count("id_venda").alias("qtd_vendas"),
    sum("total").alias("total_vendido"),
    round(avg("total"), 2).alias("ticket_medio")
).orderBy("hash_id_vendedor")

# Escrita segura
vendas_por_vendedor.write.mode("overwrite").parquet(output_path)