from pyspark.sql.functions import col, count, sum, avg, round

class FaturamentoDiario:
    def __init__(self, sparkSession):
        self.sparkSession = sparkSession
        self.input_path = './lab/jobs/data'
        self.output_path = './lab/jobs/views/faturamento_diario'

    def Run(self):
        df_vendas = self.sparkSession.spark.read.parquet(self.input_path)
        df_vendas = df_vendas.withColumn("data", col("data").cast("date"))

        faturamento_diario = df_vendas.groupBy("data").agg(
            count("id_venda").alias("qtd_vendas"),
            sum("total").alias("total_faturado"),
            round(avg("total"), 2).alias("ticket_medio")
        ).orderBy("data")

        faturamento_diario.write.mode("overwrite").parquet(self.output_path)