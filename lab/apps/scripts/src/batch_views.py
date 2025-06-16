from pyspark.sql.functions import col, count, sum, avg, max, round, sha2
from src.notificador import Notificador

class BatchViews:
    def __init__(self, sparkSession):
        self.sparkSession = sparkSession
        self.input_data = 'nessie.data'
        self.notificador = Notificador()

        # Nomes das views
        self.faturamento_diario = 'faturamento_diario'
        self.ticket_medio_cliente = 'ticket_medio_cliente'
        self.vendas_por_vendedor = 'vendas_por_vendedor'

        # Garante namespace
        self.sparkSession.sql("CREATE NAMESPACE IF NOT EXISTS nessie.batch")

        # Dispatcher interno de comandos
        self.commands = {
            'faturamento_diario': self.faturamentoDiario,
            'ticket_medio_cliente': self.ticketMedioCliente,
            'vendas_por_vendedor': self.vendasPorVendedor,
        }

    def faturamentoDiario(self):
        self.notificador.mostrar('info', f'Batch view "{self.faturamento_diario}" iniciado.')
        try:
            df_vendas = self.sparkSession.read.table(f'{self.input_data}.vendas')
            df_vendas = df_vendas.withColumn('data', col('data').cast('date'))

            result = df_vendas.groupBy('data').agg(
                max('id_venda').alias('maior_id'),
                count('id_venda').alias('qtd_vendas'),
                sum('total').alias('total_faturado'),
                round(avg('total'), 2).alias('ticket_medio')
            ).orderBy('data')

            result.writeTo(f'nessie.batch.{self.faturamento_diario}').using("iceberg").createOrReplace()
            self.notificador.mostrar('info', f'"{self.faturamento_diario}" finalizada com sucesso!\n')
        except Exception as e:
            self.notificador.mostrar('error', f'"{self.faturamento_diario}" falhou - {e}\n')

    def ticketMedioCliente(self):
        self.notificador.mostrar('info', f'Batch view "{self.ticket_medio_cliente}" iniciado.')
        try:
            df_vendas = self.sparkSession.read.table(f'{self.input_data}.vendas') \
                .withColumn('id_cliente', col('id_cliente').cast('string')) \
                .withColumn('total', col('total').cast('decimal(10,2)')) \
                .withColumn('hash_id_cliente', sha2(col('id_cliente'), 256))

            result = df_vendas.groupBy('hash_id_cliente').agg(
                max('id_venda').alias('maior_id'),
                count('id_venda').alias('qtd_vendas'),
                sum('total').alias('total_gasto'),
                round(avg('total'), 2).alias('ticket_medio')
            ).orderBy('hash_id_cliente')

            result.writeTo(f'nessie.batch.{self.ticket_medio_cliente}').using("iceberg").createOrReplace()
            self.notificador.mostrar('info', f'"{self.ticket_medio_cliente}" finalizada com sucesso!\n')
        except Exception as e:
            self.notificador.mostrar('error', f'"{self.ticket_medio_cliente}" falhou - {e}\n')

    def vendasPorVendedor(self):
        self.notificador.mostrar('info', f'Batch view "{self.vendas_por_vendedor}" iniciado.')
        try:
            df_vendas = self.sparkSession.read.table(f'{self.input_data}.vendas') \
                .withColumn('id_vendedor', col('id_vendedor').cast('string')) \
                .withColumn('total', col('total').cast('decimal(10,2)')) \
                .withColumn('hash_id_vendedor', sha2(col('id_vendedor'), 256))

            result = df_vendas.groupBy('hash_id_vendedor').agg(
                max('id_venda').alias('maior_id'),
                count('id_venda').alias('qtd_vendas'),
                sum('total').alias('total_vendido'),
                round(avg('total'), 2).alias('ticket_medio')
            ).orderBy('hash_id_vendedor')

            result.writeTo(f'nessie.batch.{self.vendas_por_vendedor}').using("iceberg").createOrReplace()
            self.notificador.mostrar('info', f'"{self.vendas_por_vendedor}" finalizada com sucesso!\n')
        except Exception as e:
            self.notificador.mostrar('error', f'"{self.vendas_por_vendedor}" falhou - {e}\n')

    def run(self, comando: str = None):
        self.notificador.mostrar('info', 'Iniciando processamento batch...\n')
        if comando:
            task = self.commands.get(comando)
            if task:
                task()
            else:
                self.notificador.mostrar('error', f'Comando "{comando}" não encontrado. Opções: {", ".join(self.commands.keys())}\n')
        else:
            # Executa todas
            for nome, task in self.commands.items():
                task()
        self.notificador.mostrar('info', 'Processamento batch finalizado.\n')
