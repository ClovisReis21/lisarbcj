import sys
import os
import findspark
import mysql.connector
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, DateType, TimestampType
)
from src.notificador import Notificador

class Extrator:
    def __init__(self, spark_session):
        self.spark_session = spark_session
        self.base_datetime = '2025-03-01 00:00:00'
        self.clean_time = 30  # dias

        self.actors_id = {
            "vendedores": "id_vendedor",
            "vendas": "id_venda",
            "produtos": "id_produto",
            "clientes": "id_cliente",
            "itens_venda": "id_venda"
        }

        self.path_new_data = 'nessie.new_data'
        self.path_new_data_map = f'{self.path_new_data}.new_data_map'
        self.notificador = Notificador()

        self.spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.path_new_data}")

    def get_last_ingesta_datetime(self, tabela):
        if self.spark_session._jsparkSession.catalog().tableExists(self.path_new_data_map):
            df = self.spark_session.read.table(self.path_new_data_map) \
                .filter(F.col("table") == tabela) \
                .orderBy("date", ascending=False)
            rows = df.collect()
            if rows:
                return rows[0]["date"]
        self.notificador.mostrar('info', f'Datetime padrão "{self.base_datetime}" para base "{tabela}"')
        return self.base_datetime

    def set_last_ingesta_datetime(self, tabela, last_date):
        path_data = f'{self.path_new_data}.{tabela}_{last_date}'.replace('-', '').replace(' ', 'T').replace(':', '')
        df = self.spark_session.createDataFrame(
            [(tabela, last_date, path_data)],
            schema=StructType([
                StructField("table", StringType(), False),
                StructField("date", StringType(), False),
                StructField("path", StringType(), False)
            ])
        )

        if self.spark_session._jsparkSession.catalog().tableExists(self.path_new_data_map):
            df.writeTo(self.path_new_data_map).append()
            self.notificador.mostrar('info', f'Metadata "{self.path_new_data_map}" atualizada.')
        else:
            df.writeTo(self.path_new_data_map).using("iceberg").createOrReplace()
            self.notificador.mostrar('info', f'Metadata "{self.path_new_data_map}" criada.')

    def get_user_info(self, last_date, tabela):
        host = sys.argv[2]
        user = sys.argv[3]
        password = sys.argv[4]
        database = sys.argv[5]

        try:
            conn = mysql.connector.connect(
                host=host, user=user, password=password, database=database
            )
            cursor = conn.cursor()
            cursor.callproc('USER_INFO', [last_date, tabela])
            for result in cursor.stored_results():
                rows = result.fetchall()
            cursor.close()
            conn.close()
            if rows:
                info = rows[0]
                self.notificador.mostrar('info', f'USER_INFO: Max={info[0]}, Low={info[1]}, Up={info[2]}, Linhas={info[3]}')
                return info
        except Exception as e:
            self.notificador.mostrar('error', f'Erro MySQL - tabela "{tabela}": {e}')
        return None

    def get_data(self, max_conn, low_bound, up_bound, last_date, tabela):
        partition_col = self.actors_id[tabela]

        condition = "criacao > '{0}'".format(last_date) if tabela in ['itens_venda', 'vendas'] else \
                    "(criacao > '{0}' OR atualizacao > '{0}')".format(last_date)

        query = f"(SELECT * FROM {tabela} WHERE {condition}) as subq"

        try:
            return (
                self.spark_session.read
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/vendas?allowPublicKeyRetrieval=true&useSSL=false")
                .option("dbtable", query)
                .option("user", sys.argv[3])
                .option("password", sys.argv[4])
                .option("numPartitions", max_conn)
                .option("partitionColumn", partition_col)
                .option("lowerBound", low_bound)
                .option("upperBound", up_bound)
                .load()
            )
        except Exception as e:
            self.notificador.mostrar('error', f'Erro ao extrair dados MySQL - "{tabela}": {e}')
            return None

    def save_data(self, df, tabela):
        valid_cols = list(set(df.columns) & {"criacao", "atualizacao"})
        last_date = (
            df.select(valid_cols)
            .withColumn("date", F.coalesce(*[F.col(c) for c in valid_cols]).cast("timestamp"))
            .agg(F.max("date").alias("last_date"))
            .collect()[0]["last_date"]
        )

        target_table = f'{self.path_new_data}.{tabela}_{last_date}'.replace('-', '').replace(' ', 'T').replace(':', '')
        try:
            df.writeTo(target_table).using("iceberg").createOrReplace()
            self.notificador.mostrar('info', f'"{target_table}" salvo com sucesso.')
            return str(last_date)
        except Exception as e:
            self.notificador.mostrar('error', f'Erro ao salvar "{target_table}": {e}')
            return self.base_datetime

    def run(self):
        self.notificador.mostrar('info', 'Iniciando extração de dados.\n')
        for tabela in self.actors_id:
            self.notificador.mostrar('info', f'Tabela atual: "{tabela}"')
            last_date = self.get_last_ingesta_datetime(tabela)
            user_info = self.get_user_info(last_date, tabela)

            if not user_info or user_info[3] == 0:
                self.notificador.mostrar('info', f'Sem dados novos em "{tabela}"\n')
                continue

            df = self.get_data(
                max_conn=user_info[0],
                low_bound=user_info[1],
                up_bound=user_info[2],
                last_date=last_date,
                tabela=tabela
            )

            if df is None or df.count() == 0:
                self.notificador.mostrar('info', f'Nenhum registro encontrado em "{tabela}"\n')
                continue

            last_date = self.save_data(df, tabela)
            self.set_last_ingesta_datetime(tabela, last_date)
            self.notificador.mostrar('info', f'Extração de "{tabela}" concluída.\n')

        self.notificador.mostrar('info', f'Extração finalizada para todas as tabelas.\n')
        self.spark_session.stop()
