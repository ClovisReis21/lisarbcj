import os
from spark import Spark
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType, LongType
from pyspark.sql.functions import col, sum, from_json, unix_timestamp, window
from pyspark.sql.functions import col, count, sum, avg, max , round, sha2
from notificador import Notificador

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

class SpeedViews:
    def __init__(self, sparkSession):
        self.sparkSession = sparkSession
        self.notificador = Notificador()
        self.batch_path = "/home/cj/lisarb_jc/lab/jobs/views/batch/faturamento_diario"

    def GetKafkaSchema(self):
        return StructType([
            StructField("id_vendedor", IntegerType(), False),
            StructField("id_cliente", IntegerType(), False),
            StructField("id_produto", IntegerType(), False),
            StructField("id_venda", IntegerType(), False),
            StructField("quantidade", IntegerType(), False),
            StructField("valor_unitario", DoubleType(), False),
            StructField("valor_total", DoubleType(), False),
            StructField("desconto", DoubleType(), False),
            StructField("data", DateType(), False)
        ])

    def GetVwTicketMedioCienteSchema(self):
        return StructType([
            StructField("hash_id_cliente", StringType(), True),
            StructField("qtd_vendas", LongType(), True),
            StructField("total_gasto", DoubleType(), True),
            StructField("ticket_medio", DoubleType(), True)
        ])
    
    def ObterMaiorIdVenda(self):
        try:
            if os.path.exists(self.batch_path):
                df_batch = self.sparkSession.spark.read.parquet(self.batch_path)
                maior_id = (df_batch
                    .sort(col('maior_id'), ascending=False)
                    .limit(1)
                    .select(col('maior_id'))
                    .collect()[0][0])
                print('maior_id:', maior_id)
                return maior_id
            else:
                self.notificador.Mostrar("info", "Nenhum dado batch encontrado. Considerando id_venda = 0.")
                print('ObterMaiorIdVenda(self) - fim 1')
                return 0
        except Exception as e:
            self.notificador.Mostrar("error", f"Erro ao obter maior id_venda da batch: {e}")
            print('ObterMaiorIdVenda(self) - fim 2')
            return 0
    
    def Run(self):
        self.notificador.Mostrar('info', f'Iniciando subcrição no kafka...\n')

        # 1. Obtem maior id_venda da batch
        maior_id_venda_batch = self.ObterMaiorIdVenda()

        # return

        df_stream = self.sparkSession.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "vendas-deshboard-bronze") \
            .load()
        
        df_base = (
            df_stream.selectExpr("CAST(value AS STRING)", "timestamp")
            .select(from_json(col("value"), self.GetKafkaSchema()).alias("data"), col("timestamp"))
            .select("data.*", "timestamp")
            .withColumn("hash_id_cliente", sha2(col("id_cliente").cast("string"), 256))
            .withColumn("hash_id_vendedor", sha2(col("id_vendedor").cast("string"), 256))
            .filter(col("id_venda") > maior_id_venda_batch)
        )

        df_base.printSchema()

        # View 2: Faturamento diario
        faturamento_diario = (df_base
            .withWatermark("timestamp", "10 seconds")
            .groupBy(window(col("timestamp"), "10 seconds"), col("data")).agg(
                max('id_venda').alias('maior_id'),
                count('id_venda').alias('qtd_vendas'),
                sum('valor_total').alias('total_faturado'),
                round(avg('valor_total'), 2).alias('ticket_medio')
                )
            )
        # Cada uma dessas views pode ter sua própria escrita:
        to_faturamento_diario = (faturamento_diario.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", "/home/cj/lisarb_jc/lab/jobs/views/speed/faturamento_diario")
            .option("checkpointLocation", "/home/cj/lisarb_jc/lab/jobs/views/speed/checkpoints_fd")
            .start())  

        # Agora você deriva quantas views quiser a partir de df_base:
        # View 2: Ticket médio por cliente
        ticket_medio_cliente = (df_base
            .withWatermark("timestamp", "10 seconds")
            .groupBy(window(col("timestamp"), "10 seconds"), col("hash_id_cliente")).agg(
                max('id_venda').alias('maior_id'),
                count("id_venda").alias("qtd_vendas"),
                sum("valor_total").alias("total_gasto"),
                round(avg("valor_total"), 2).alias("ticket_medio")
                )
            )
        # Cada uma dessas views pode ter sua própria escrita:
        to_ticket_medio_cliente = (ticket_medio_cliente.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", "/home/cj/lisarb_jc/lab/jobs/views/speed/ticket_medio_cliente")
            .option("checkpointLocation", "/home/cj/lisarb_jc/lab/jobs/views/speed/checkpoints_tmc")
            .start())     


        # View 3: Vendas por vendedor
        vendas_por_vendedor = (df_base
            .withWatermark("timestamp", "10 seconds")
            .groupBy(window(col("timestamp"), "10 seconds"), col("hash_id_vendedor")).agg(
                max('id_venda').alias('maior_id'),
                count("id_venda").alias("qtd_vendas"),
                sum("valor_total").alias("total_vendido"),
                round(avg("valor_total"), 2).alias("ticket_medio")
                )
            )
        # Cada uma dessas views pode ter sua própria escrita:
        to_vendas_por_vendedor = (vendas_por_vendedor.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", "/home/cj/lisarb_jc/lab/jobs/views/speed/vendas_por_vendedor")
            .option("checkpointLocation", "/home/cj/lisarb_jc/lab/jobs/views/speed/checkpoints_vpc")
            .start())
        
        to_faturamento_diario.awaitTermination()
        to_ticket_medio_cliente.awaitTermination()
        to_vendas_por_vendedor.awaitTermination()

sparkSession = Spark('speed')
SpeedViews(sparkSession).Run()