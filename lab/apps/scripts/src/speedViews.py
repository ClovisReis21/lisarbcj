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
        self.batch_path = "nessie.batch.faturamento_diario"
        self.sparkSession.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.speed")
        self.sparkSession.sql("""
            CREATE TABLE IF NOT EXISTS nessie.speed.faturamento_diario (
                janela_inicio TIMESTAMP,
                janela_fim TIMESTAMP,
                data DATE,
                maior_id BIGINT,
                qtd_vendas BIGINT,
                total_faturado DOUBLE,
                ticket_medio DOUBLE
            )
            USING iceberg
            PARTITIONED BY (days(janela_inicio));
        """)
        self.sparkSession.sql("""
            CREATE TABLE IF NOT EXISTS nessie.speed.ticket_medio_cliente (
                janela_inicio TIMESTAMP,
                janela_fim TIMESTAMP,
                hash_id_cliente STRING,
                maior_id BIGINT,
                qtd_vendas BIGINT,
                total_gasto DOUBLE,
                ticket_medio DOUBLE
            )
            USING iceberg
            PARTITIONED BY (days(janela_inicio));
        """)
        self.sparkSession.sql("""
            CREATE TABLE IF NOT EXISTS nessie.speed.vendas_por_vendedor (
                janela_inicio TIMESTAMP,
                janela_fim TIMESTAMP,
                hash_id_vendedor STRING,
                maior_id BIGINT,
                qtd_vendas BIGINT,
                total_vendido DOUBLE,
                ticket_medio DOUBLE
            )
            USING iceberg
            PARTITIONED BY (days(janela_inicio));
        """)

    def getKafkaSchema(self):
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

    def getVwTicketMedioCienteSchema(self):
        return StructType([
            StructField("hash_id_cliente", StringType(), True),
            StructField("qtd_vendas", LongType(), True),
            StructField("total_gasto", DoubleType(), True),
            StructField("ticket_medio", DoubleType(), True)
        ])
    
    def obterMaiorIdVenda(self):
        try:
            if self.sparkSession._jsparkSession.catalog().tableExists(self.batch_path):
                df_batch = self.sparkSession.read.table(self.batch_path)
                maior_id = (df_batch
                    .sort(col('maior_id'), ascending=False)
                    .limit(1)
                    .select(col('maior_id'))
                    .collect()[0][0])
                print('maior_id:', maior_id)
                return maior_id
            else:
                self.notificador.mostrar("info", "Nenhum dado batch encontrado. Considerando id_venda = 0.")
                print('ObterMaiorIdVenda(self) - fim 1')
                return 0
        except Exception as e:
            self.notificador.mostrar("error", f"Erro ao obter maior id_venda da batch: {e}")
            print('ObterMaiorIdVenda(self) - fim 2')
            return 1
    
    def Run(self):
        self.notificador.mostrar('info', f'Iniciando subcrição no kafka...\n')

        maior_id_venda_batch = self.obterMaiorIdVenda()

        df_stream = self.sparkSession.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "vendas-deshboard-bronze") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        df_base = (
            df_stream.selectExpr("CAST(value AS STRING)", "timestamp")
            .select(from_json(col("value"), self.getKafkaSchema()).alias("data"), col("timestamp"))
            .select("data.*", "timestamp")
            .withColumn("hash_id_cliente", sha2(col("id_cliente").cast("string"), 256))
            .withColumn("hash_id_vendedor", sha2(col("id_vendedor").cast("string"), 256))
            .filter(col("id_venda") > maior_id_venda_batch)
        )

        df_base.printSchema()

        faturamento_diario = (df_base
            .withWatermark("timestamp", "10 seconds")
            .groupBy(window(col("timestamp"), "10 seconds"), col("data")
            ).agg(
                max('id_venda').alias('maior_id'),
                count('id_venda').alias('qtd_vendas'),
                sum('valor_total').alias('total_faturado'),
                round(avg('valor_total'), 2).alias('ticket_medio')
            ).select(
                col('window.start').alias('janela_inicio'),
                col('window.end').alias('janela_fim'),
                col('data'),
                col('maior_id'),
                col('qtd_vendas'),
                col('total_faturado'),
                col('ticket_medio')
            )
        )
        to_faturamento_diario = (faturamento_diario.writeStream
            .outputMode("append")
            .format("iceberg")
            .option("checkpointLocation", "/home/cj/lisarbcj/lab/jobs/views/speed/checkpoints_fd")
            .toTable("nessie.speed.faturamento_diario")
        )

        ticket_medio_cliente = (df_base
            .withWatermark("timestamp", "10 seconds")
            .groupBy(window(col("timestamp"), "10 seconds"), col("hash_id_cliente")).agg(
                max('id_venda').alias('maior_id'),
                count("id_venda").alias("qtd_vendas"),
                sum("valor_total").alias("total_gasto"),
                round(avg("valor_total"), 2).alias("ticket_medio")
                )
            ).select(
                col('window.start').alias('janela_inicio'),
                col('window.end').alias('janela_fim'),
                col('hash_id_cliente'),
                col('maior_id'),
                col('qtd_vendas'),
                col('total_gasto'),
                col('ticket_medio')
            )
        to_ticket_medio_cliente = (ticket_medio_cliente.writeStream
            .outputMode("append")
            .format("iceberg")
            .option("checkpointLocation", "/home/cj/lisarbcj/lab/jobs/views/speed/checkpoints_tmc")
            .toTable("nessie.speed.ticket_medio_cliente")
        )

        vendas_por_vendedor = (df_base
            .withWatermark("timestamp", "10 seconds")
            .groupBy(window(col("timestamp"), "10 seconds"), col("hash_id_vendedor")).agg(
                max('id_venda').alias('maior_id'),
                count("id_venda").alias("qtd_vendas"),
                sum("valor_total").alias("total_vendido"),
                round(avg("valor_total"), 2).alias("ticket_medio")
                )
            ).select(
                col('window.start').alias('janela_inicio'),
                col('window.end').alias('janela_fim'),
                col('hash_id_vendedor'),
                col('maior_id'),
                col('qtd_vendas'),
                col('total_vendido'),
                col('ticket_medio')
            )
        to_vendas_por_vendedor = (vendas_por_vendedor.writeStream
            .outputMode("append")
            .format("iceberg")
            .option("checkpointLocation", "/home/cj/lisarbcj/lab/jobs/views/speed/checkpoints_vpc")
            .toTable("nessie.speed.vendas_por_vendedor")
        )
        
        to_faturamento_diario.awaitTermination()
        to_ticket_medio_cliente.awaitTermination()
        to_vendas_por_vendedor.awaitTermination()
