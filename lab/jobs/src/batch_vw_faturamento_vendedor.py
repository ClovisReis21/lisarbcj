from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, round, sha2

# Inicializa Spark
spark = SparkSession.builder \
    .appName("TicketMedioClienteAnonimo") \
    .getOrCreate()

# Caminhos
input_path = "/data/vendas"
output_path = "/output/ticket_medio_cliente_anon"

# Leitura dos dados em Parquet
df_vendas = spark.read.parquet(input_path)

# Garantir que os campos estão no tipo correto
df_vendas = df_vendas \
    .withColumn("id_cliente", col("id_cliente").cast("string")) \
    .withColumn("total", col("total").cast("decimal(10,2)"))

# Anonimizar cliente
df_vendas = df_vendas.withColumn("hash_id_cliente", sha2(col("id_cliente"), 256))

# Agregação com cliente anonimizado
ticket_medio_cliente = df_vendas.groupBy("hash_id_cliente").agg(
    count("id_venda").alias("qtd_vendas"),
    sum("total").alias("total_gasto"),
    round(avg("total"), 2).alias("ticket_medio")
).orderBy("hash_id_cliente")

# Escrita segura em Parquet
ticket_medio_cliente.write.mode("overwrite").parquet(output_path)