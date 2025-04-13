from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, round, sha2

# Inicializa Spark
spark = SparkSession.builder \
    .appName("TicketMedioPorCliente") \
    .getOrCreate()

# Caminho de entrada e saída
input_path = "/data/vendas"
output_path = "/output/ticket_medio_cliente"

# Leitura dos dados de vendas em Parquet
df_vendas = spark.read.parquet(input_path)

# Garante que as colunas estão no formato correto
df_vendas = df_vendas.withColumn("id_cliente", col("id_cliente").cast("string")) \
                     .withColumn("total", col("total").cast("decimal(10,2)"))

# Anonimizar cliente
df_vendas = df_vendas.withColumn("hash_id_cliente", sha2(col("id_cliente"), 256))

# Agregação: Ticket médio por cliente
ticket_medio_cliente = df_vendas.groupBy("hash_id_cliente").agg(
    count("id_venda").alias("qtd_vendas"),
    sum("total").alias("total_gasto"),
    round(avg("total"), 2).alias("ticket_medio")
).orderBy("hash_id_cliente")

df_clientes = spark.read.parquet("/data/clientes")

ticket_medio_cliente = ticket_medio_cliente.join(
    df_clientes.select("id_cliente", "cliente", "estado", "sexo"),
    on="id_cliente",
    how="left"
)

# Escrita do resultado em Parquet
ticket_medio_cliente.write.mode("overwrite").parquet(output_path)