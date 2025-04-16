import os, findspark
import pyspark.sql.functions as F
from src.notificador import Notificador
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType, DateType

findspark.add_packages('mysql:mysql-connector-java:8.0.11')

class Ingesta:
    def __init__(self, sparkSession):
        self.dir_path = os.getcwd()
        self.sparkSession = sparkSession
        self.firstDatetime = '2025-03-01 00:00:00.000'
        self.cleanTime = 30
        self.actors_id = {
            "vendedores": "id_vendedor",
            "vendas": "id_venda",
            "produtos": "id_produto",
            "clientes": "id_cliente",
            "itens_venda": "id_venda"
        }
        self.pathNewDataMap = './lab/jobs/new_data/new_data_map'
        self.pathNewData = './lab/jobs/new_data'
        self.pathData = './lab/jobs/data'
        self.notificador = Notificador()

    def GetLastIngestaDatetime(self, tabela):
        pathData = f'{self.pathData}/{tabela}'
        os.makedirs(pathData, exist_ok=True)
        try:
            masterDataSet_df = self.sparkSession.spark.read.parquet(f'{pathData}')
            valid_columns = list(set(masterDataSet_df.columns) & set(['atualizacao', 'criacao']))
            last_datetime = (masterDataSet_df
                .select(valid_columns)
                .withColumn('date', F.coalesce(valid_columns[0], valid_columns[-1]).cast('timestamp'))
                .drop(*valid_columns)
                .select(F.max(F.col('date')))
            ).collect()[0][0]
            self.notificador.Mostrar('info', f'Maior data encontrada no Master Dataset {last_datetime}')
            return str(last_datetime)
        except Exception as e:
            self.notificador.Mostrar('info', f'Master Dataset "{tabela}" não encontrado - {e}')
            return self.firstDatetime
        
    def GetNewData(self, tabela, last_datetime): # Read from MySQL Table
        newDataMap_df = None
        try:
            newDataMap_df = (
                self.sparkSession.spark.read.parquet(f'{self.pathNewDataMap}')
                    .where((F.col('table') == tabela) &
                        (F.to_timestamp(F.col('date')).alias('date') > F.to_timestamp(F.lit(last_datetime)).alias('date')))
                    .sort('date'))
        except Exception as e:
            self.notificador.Mostrar('info', f'New data para tabela "new_data_map" não encontrada - {e}\n')
            return {"full_response_df": None, "pathList": []}
        pathList = []
        rows = newDataMap_df.collect()
        for row in rows:
            pathList.append(row['path'])
        full_response_df = self.sparkSession.spark.createDataFrame([], self.GetSchema(tabela))
        for path in pathList:
            try:
                response_df = (self.sparkSession.spark.read.parquet(f'{path}'))
                full_response_df = full_response_df.unionByName(response_df)
            except Exception as e:
                self.notificador.Mostrar('info', f'New data para tabela "{tabela}" não encontrada - {e}\n')
                return {"full_response_df": None, "pathList": []}
        self.notificador.Mostrar('info', f'{full_response_df.count()} registros encontrados para base "{tabela}"')
        return {"full_response_df": full_response_df, "pathList": pathList}

    def SaveData(self, df, tabela):
        strTableDir = f'{self.pathData}/{tabela}'
        try:
            df.write.mode("overwrite").parquet(strTableDir)
            self.notificador.Mostrar('info', f'Master Dataset "{tabela}" salvo com sucesso\n')
            return 0
        except Exception as e:
            self.notificador.Mostrar('info', f'Master Dataset "{tabela}" com erro ao salvar - {e}')
            return 1

    def GetSchema(self, schemaName):
        schemas = {}
        schemas['vendedores'] = StructType([
            StructField("id_vendedor", IntegerType(),True),
            StructField("cpf", StringType(),True),
            StructField("telefone", StringType(),True),
            StructField("email", StringType(), True),
            StructField("origem_racial", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("criacao", TimestampType(), True),
            StructField("atualizacao", TimestampType(), True),
        ])
        schemas['vendas'] = StructType([
            StructField("id_venda", IntegerType(),True),
            StructField("id_vendedor", IntegerType(),True),
            StructField("id_cliente", IntegerType(),True),
            StructField("data", DateType(), True),
            StructField("total", DecimalType(10,2), True),
            StructField("criacao", TimestampType(), True),
        ])
        schemas['produtos'] = StructType([
            StructField("id_produto", IntegerType(),True),
            StructField("produto", StringType(),True),
            StructField("preco", DecimalType(10,2),True),
            StructField("criacao", TimestampType(), True),
            StructField("atualizacao", TimestampType(), True),
        ])
        schemas['clientes'] = StructType([
            StructField("id_cliente", IntegerType(),True),
            StructField("cpf", StringType(),True),
            StructField("telefone", StringType(),True),
            StructField("email", StringType(), True),
            StructField("cliente", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("origem_racial", StringType(), True),
            StructField("sexo", StringType(), True),
            StructField("status", StringType(), True),
            StructField("criacao", TimestampType(), True),
            StructField("atualizacao", TimestampType(), True),
        ])
        schemas['itens_venda'] = StructType([
            StructField("id_produto", IntegerType(),True),
            StructField("id_venda", IntegerType(),True),
            StructField("quantidade", IntegerType(),True),
            StructField("valor_unitario", DecimalType(10,2), True),
            StructField("valor_total", DecimalType(10,2), True),
            StructField("desconto", DecimalType(10,2), True),
            StructField("criacao", TimestampType(), True),
        ])
        schemas['new_data_map'] = StructType([
            StructField("table", StringType(),True),
            StructField("date", StringType(),True),
            StructField("path", StringType(),True),
        ])
        return schemas[schemaName]

    def Run(self):
        tabelas = list(self.actors_id.keys())
        for tabela in tabelas:
            self.notificador.Mostrar('info', f'Iniciando ingestão da tabela "{tabela}"')
            last_date = self.GetLastIngestaDatetime(tabela)
            newData = self.GetNewData(tabela, last_date)
            pathList = newData['full_response_df']
            last_date = newData['pathList']
            if pathList == None or pathList.count() == 0:
                continue
            self.SaveData(pathList, tabela)