from pyspark.sql.functions import col, count, sum, avg, round, sha2
from src.notificador import Notificador

class BatchViews:
    def __init__(self, sparkSession):
        self.sparkSession = sparkSession
        self.input_data ='./data/vendas'
        self.faturamento_diario = 'faturamento_diario'
        self.ticket_medio_cliente = 'ticket_medio_cliente'
        self.vendas_por_vendedor = 'vendas_por_vendedor'
        self.notificador = Notificador()
        # self.Run()

    def FaturamentoDiario(self):
        self.notificador.Mostrar('info', f'Batch view "{self.faturamento_diario}" iniciado.')
        try:
            df_vendas = self.sparkSession.spark.read.parquet(self.input_data)
            df_vendas = df_vendas.withColumn('data', col('data').cast('date'))

            faturamento_diario = df_vendas.groupBy('data').agg(
                count('id_venda').alias('qtd_vendas'),
                sum('total').alias('total_faturado'),
                round(avg('total'), 2).alias('ticket_medio')
            ).orderBy('data')
            faturamento_diario.write.mode('overwrite').parquet(f'./views/{self.faturamento_diario}')
            self.notificador.Mostrar('info', f'Batch view "{self.faturamento_diario}" finalizada com sucesso!\n')
        except Exception as e:
            self.notificador.Mostrar('error', f'"{self.faturamento_diario}" não processada. - {e}\n')

    def TicketMedioCliente(self):
        self.notificador.Mostrar('info', f'Batch view "{self.ticket_medio_cliente}" iniciado.')
        try:
            df_vendas = self.sparkSession.spark.read.parquet(self.input_data)
            df_vendas = df_vendas.withColumn('id_cliente', col('id_cliente').cast('string')) \
                                .withColumn('total', col('total').cast('decimal(10,2)'))

            df_vendas = df_vendas.withColumn('hash_id_cliente', sha2(col('id_cliente'), 256))
            ticket_medio_cliente = df_vendas.groupBy('hash_id_cliente').agg(
                count('id_venda').alias('qtd_vendas'),
                sum('total').alias('total_gasto'),
                round(avg('total'), 2).alias('ticket_medio')
            ).orderBy('hash_id_cliente')
            ticket_medio_cliente.write.mode('overwrite').parquet(f'./views/{self.ticket_medio_cliente}')
            self.notificador.Mostrar('info', f'Batch view "{self.ticket_medio_cliente}" finalizada com sucesso!\n')
        except Exception as e:
            self.notificador.Mostrar('error', f'"{self.ticket_medio_cliente}" não processada. - {e}\n')

    def VendasPorVendedor(self):
        self.notificador.Mostrar('info', f'Batch view "{self.vendas_por_vendedor}" iniciado.')
        try:
            df_vendas = self.sparkSession.spark.read.parquet(self.input_data)
            df_vendas = df_vendas \
                .withColumn("id_vendedor", col("id_vendedor").cast("string")) \
                .withColumn("total", col("total").cast("decimal(10,2)"))

            df_vendas = df_vendas.withColumn("hash_id_vendedor", sha2(col("id_vendedor"), 256))
            vendas_por_vendedor = df_vendas.groupBy("hash_id_vendedor").agg(
                count("id_venda").alias("qtd_vendas"),
                sum("total").alias("total_vendido"),
                round(avg("total"), 2).alias("ticket_medio")
            ).orderBy("hash_id_vendedor")
            vendas_por_vendedor.write.mode("overwrite").parquet(f'./views/{self.vendas_por_vendedor}')
            self.notificador.Mostrar('info', f'Batch view "{self.vendas_por_vendedor}" finalizada com sucesso!\n')
        except Exception as e:
            self.notificador.Mostrar('error', f'"{self.vendas_por_vendedor}" não processada. - {e}\n')
    
    def Run(self):
        self.notificador.Mostrar('info', f'Iniciando processo bath view...\n')
        self.FaturamentoDiario()
        self.TicketMedioCliente()
        self.VendasPorVendedor()
        self.notificador.Mostrar('info', f'Processo bath view finalizado')