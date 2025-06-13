import os
import findspark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, DecimalType, DateType
)
from src.notificador import Notificador

class Ingesta:
    def __init__(self, spark_session):
        self.spark_session = spark_session
        self.dir_path = os.getcwd()
        self.first_datetime = '2025-03-01 00:00:00'
        self.clean_time = 30  # dias

        self.bases = {
            "vendedores": "id_vendedor",
            "vendas": "id_venda",
            "produtos": "id_produto",
            "clientes": "id_cliente",
            "itens_venda": "id_venda"
        }

        self.path_new_data = 'nessie.new_data'
        self.path_new_data_map = f'{self.path_new_data}.new_data_map'
        self.path_data = 'nessie.data'
        self.notificador = Notificador()

        # Garante que o namespace exista
        self.spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.path_data}")

    def get_last_ingesta_datetime(self, tabela):
        full_table = f'{self.path_data}.{tabela}'
        if self.spark_session._jsparkSession.catalog().tableExists(full_table):
            df = self.spark_session.read.table(full_table)
            valid_cols = list(set(df.columns) & {"atualizacao", "criacao"})
            if valid_cols:
                timestamp_col = F.coalesce(*[F.col(col) for col in valid_cols]).cast("timestamp")
                latest = df.withColumn("ref_date", timestamp_col).agg(F.max("ref_date")).collect()
                if latest and latest[0][0]:
                    self.notificador.mostrar("info", f'Maior data no Master Dataset: {latest[0][0]}')
                    return str(latest[0][0])
        self.notificador.mostrar("info", f'Tabela "{tabela}" não encontrada. Usando data inicial.')
        return self.first_datetime

    def get_new_data(self, tabela, last_datetime):
        if not self.spark_session._jsparkSession.catalog().tableExists(self.path_new_data_map):
            self.notificador.mostrar("info", f'New data map não encontrado para tabela "{tabela}".')
            return {"full_response_df": None, "path_list": []}

        df_map = self.spark_session.read.table(self.path_new_data_map)
        filtered = df_map.filter(
            (F.col("table") == tabela) &
            (F.to_timestamp("date") > F.to_timestamp(F.lit(last_datetime)))
        ).sort("date")

        rows = filtered.collect()
        if not rows:
            self.notificador.mostrar("info", f'Nenhum novo dado encontrado para "{tabela}".')
            return {"full_response_df": None, "path_list": []}

        path_list = [row["path"] for row in rows]
        full_df = self.spark_session.createDataFrame([], self.get_schema(tabela))

        for path in path_list:
            if self.spark_session._jsparkSession.catalog().tableExists(path):
                df = self.spark_session.read.table(path)
                full_df = full_df.unionByName(df)

        self.notificador.mostrar("info", f'{full_df.count()} registros encontrados para "{tabela}".')
        return {"full_response_df": full_df, "path_list": path_list}

    def save_data(self, df, tabela):
        target_table = f'{self.path_data}.{tabela}'
        if self.spark_session._jsparkSession.catalog().tableExists(target_table):
            df.writeTo(target_table).append()
            self.notificador.mostrar("info", f'Master Dataset "{target_table}" atualizado com sucesso.')
        else:
            df.writeTo(target_table).using("iceberg").createOrReplace()
            self.notificador.mostrar("info", f'Master Dataset "{target_table}" criado com sucesso.')

    def get_schema(self, tabela):
        schemas = {
            'vendedores': StructType([
                StructField("id_vendedor", IntegerType(), True),
                StructField("cpf", StringType(), True),
                StructField("telefone", StringType(), True),
                StructField("email", StringType(), True),
                StructField("origem_racial", StringType(), True),
                StructField("nome", StringType(), True),
                StructField("criacao", TimestampType(), True),
                StructField("atualizacao", TimestampType(), True),
            ]),
            'vendas': StructType([
                StructField("id_venda", IntegerType(), True),
                StructField("id_vendedor", IntegerType(), True),
                StructField("id_cliente", IntegerType(), True),
                StructField("data", DateType(), True),
                StructField("total", DecimalType(10, 2), True),
                StructField("criacao", TimestampType(), True),
            ]),
            'produtos': StructType([
                StructField("id_produto", IntegerType(), True),
                StructField("produto", StringType(), True),
                StructField("preco", DecimalType(10, 2), True),
                StructField("criacao", TimestampType(), True),
                StructField("atualizacao", TimestampType(), True),
            ]),
            'clientes': StructType([
                StructField("id_cliente", IntegerType(), True),
                StructField("cpf", StringType(), True),
                StructField("telefone", StringType(), True),
                StructField("email", StringType(), True),
                StructField("cliente", StringType(), True),
                StructField("estado", StringType(), True),
                StructField("origem_racial", StringType(), True),
                StructField("sexo", StringType(), True),
                StructField("status", StringType(), True),
                StructField("criacao", TimestampType(), True),
                StructField("atualizacao", TimestampType(), True),
            ]),
            'itens_venda': StructType([
                StructField("id_produto", IntegerType(), True),
                StructField("id_venda", IntegerType(), True),
                StructField("quantidade", IntegerType(), True),
                StructField("valor_unitario", DecimalType(10, 2), True),
                StructField("valor_total", DecimalType(10, 2), True),
                StructField("desconto", DecimalType(10, 2), True),
                StructField("criacao", TimestampType(), True),
            ]),
            'new_data_map': StructType([
                StructField("table", StringType(), True),
                StructField("date", StringType(), True),
                StructField("path", StringType(), True),
            ]),
        }
        return schemas[tabela]

    def run(self):
        for tabela in self.bases.keys():
            self.notificador.mostrar("info", f'Iniciando ingestão para tabela: "{tabela}"\n')
            last_datetime = self.get_last_ingesta_datetime(tabela)
            new_data = self.get_new_data(tabela, last_datetime)
            df = new_data["full_response_df"]

            if df is None or df.count() == 0:
                self.notificador.mostrar("info", f'Nenhum dado novo para "{tabela}". Ignorando...\n')
                continue

            self.save_data(df, tabela)
