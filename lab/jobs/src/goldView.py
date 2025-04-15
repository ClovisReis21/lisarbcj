from pyspark.sql.functions import col, max, count, sum, round, avg
from pyspark.sql import SparkSession
from notificador import Notificador
from spark import Spark


class GoldViews:
    def __init__(self, sparkSession):
        self.sparkSession = sparkSession
        self.notificador = Notificador()

    def gerar_view_gold_faturamento_diario(self, sparkSession):
        df_batch = sparkSession.spark.read.parquet('./views/batch/faturamento_diario')
        maior_id_batch = df_batch.agg(max('maior_id')).collect()[0][0]
        df_stream = sparkSession.spark.read.parquet('./views/speed/faturamento_diario').drop('window')
        df_stream_filtrado = df_stream.filter(col('maior_id') > maior_id_batch)
        df_unificado = df_batch.unionByName(df_stream_filtrado)
        df_gold = df_unificado.groupBy('data').agg(
            sum('qtd_vendas').alias('qtd_vendas'),
            sum('total_faturado').cast('long').alias('total_faturado'),
            round(avg('ticket_medio'), 2).alias('ticket_medio')
        ).orderBy('data')
        df_gold.write.mode('overwrite').parquet('./views/gold/faturamento_diario')
        self.notificador.Mostrar('info', f'Gold View de Faturamento Diário gerada com sucesso.')


    def gerar_view_gold_ticket_medio_cliente(self, sparkSession):
        df_batch = sparkSession.spark.read.parquet('./views/batch/ticket_medio_cliente')
        maior_id_batch = df_batch.agg(max('maior_id')).collect()[0][0]
        df_stream = sparkSession.spark.read.parquet('./views/speed/ticket_medio_cliente').drop('window')
        df_stream_filtrado = df_stream.filter(col('maior_id') > maior_id_batch)
        df_unificado = df_batch.unionByName(df_stream_filtrado)
        df_gold = df_unificado.groupBy('hash_id_cliente').agg(
            sum('qtd_vendas').alias('qtd_vendas'),
            sum('total_gasto').cast('long').alias('total_gasto'),
            round(avg('ticket_medio'), 2).alias('ticket_medio')
        )
        df_gold.write.mode('overwrite').parquet('./views/gold/ticket_medio_cliente')
        self.notificador.Mostrar('info', f'Gold View de Ticket Médio por Cliente gerada com sucesso.')

    def gerar_view_gold_vendas_por_vendedor(self, sparkSession):
        df_batch = sparkSession.spark.read.parquet('./views/batch/vendas_por_vendedor')
        maior_id_batch = df_batch.agg(max('maior_id')).collect()[0][0]
        df_stream = sparkSession.spark.read.parquet('./views/speed/vendas_por_vendedor').drop('window')
        df_stream_filtrado = df_stream.filter(col('maior_id') > maior_id_batch)
        df_unificado = df_batch.unionByName(df_stream_filtrado)
        df_gold = df_unificado.groupBy('hash_id_vendedor').agg(
            sum('qtd_vendas').alias('qtd_vendas'),
            sum('total_vendido').cast('long').alias('total_vendido'),
            round(avg('ticket_medio'), 2).alias('ticket_medio')
        )
        df_gold.write.mode('overwrite').parquet('./views/gold/vendas_por_vendedor')
        self.notificador.Mostrar('info', f'Gold View de Vendas por Vendedor gerada com sucesso.')

    def Run(self):
        self.gerar_view_gold_faturamento_diario(self.sparkSession)
        self.gerar_view_gold_ticket_medio_cliente(self.sparkSession)
        self.gerar_view_gold_vendas_por_vendedor(self.sparkSession)

sparkSession = Spark('gold')
GoldViews(sparkSession).Run()
