from datetime import datetime #, pytz
import sys, os, findspark, mysql.connector
import pyspark.sql.functions as F
from src.notificador import Notificador
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType, DateType

findspark.add_packages('mysql:mysql-connector-java:8.0.11')

class Ingesta:
    def __init__(self, sparkSession):
        self.dir_path = os.getcwd()
        self.sparkSession = sparkSession
        self.firstDatetime = '2025-03-01 00:00:00.000'
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
        print(f'{self.pathNewData}/{tabela}')
        try:
            masterDataSet_df = self.sparkSession.spark.read.parquet(f'{pathData}')
            print('masterDataSet_df:', masterDataSet_df)
            valid_columns = list(set(masterDataSet_df.columns) & set(['atualizacao', 'criacao']))
            print('valid_columns:', valid_columns)
            last_datetime = (masterDataSet_df
                .select(valid_columns)
                .withColumn('date', F.coalesce(valid_columns[0], valid_columns[-1]).cast('timestamp'))
                .drop(valid_columns)
                .select(F.max(F.col('date')))
            ).collect()[0][0]
            self.notificador.Mostrar('info', f'Maior data encontrada no Master data set: {last_datetime}')
            return str(last_datetime)
        except Exception as e:
            print(f'erro: {e}')
            self.notificador.Mostrar('info', f'Master data set "{tabela}" não encontrado - {e}')
            return self.firstDatetime
        
    def GetNewData(self, tabela, last_datetime): # Read from MySQL Table
        # LER NEW_DATA_MAP COM FILTRO DE DATA MAIOR QUE 'last_datetime'
        newDataMap_df = None
        try:
            newDataMap_df = (self.sparkSession.spark.read.parquet(f'{self.pathNewDataMap}')
                            .where((F.col('table') == tabela) &
                                (F.to_timestamp(F.col('date')).alias('date') > F.to_timestamp(F.lit(last_datetime)).alias('date')))
                            .sort('date'))
            newDataMap_df.show(100, truncate=False)
        except Exception as e:
            self.notificador.Mostrar('info', f'New data para tabela: "new_data_map" não encontrada - {e}')
            return []

        # TRASFORMAR O CAMPO PATH EM LISTA
        pathList = []
        rows = newDataMap_df.collect()
        for row in rows:
            pathList.append(row['path'])
        print('pathList:', pathList)
        full_response_df = self.sparkSession.spark.createDataFrame([], self.GetSchema(tabela))
        full_response_df.printSchema()
        for path in pathList:
            try:
                response_df = (self.sparkSession.spark.read.parquet(f'{path}'))
                # response_df.show(100, truncate=False)
                full_response_df = full_response_df.unionByName(response_df)
            except Exception as e:
                self.notificador.Mostrar('info', f'New data para tabela: "{tabela}" não encontrada - {e}')
                return []    
        return full_response_df


        # RETORNAR COM A UNIÃO DAS BASES EXISTENTES

        pathNewData = f'{self.pathNewData}/{tabela}'
        ingestas = os.listdir(pathNewData)
        if last_datetime != None:
            print('last_datetime:', last_datetime)
            last_datetime_date = list(map(int, str(last_datetime).split(' ')[0].split('-')))
            last_datetime_time = list(map(int, str(last_datetime).split(' ')[1].split(':')))
            print(last_datetime_date)
            print(last_datetime_time)
            datetimes_list = []
            for ingesta in ingestas:
                print('ingesta:',ingesta)
                ingesta_date = list(map(int, str(ingesta).split(' ')[0].split('-')))
                ingesta_time = list(map(int, str(ingesta).split(' ')[1].split(':')))
                print(ingesta_date)
                print(ingesta_time)
                if ingesta_date[0] >= last_datetime_date[0] & \
                   ingesta_date[1] >= last_datetime_date[1] & \
                   ingesta_date[2] >= last_datetime_date[2] & \
                   ingesta_time[0] >= last_datetime_time[0] & \
                   ingesta_time[1] >= last_datetime_time[1] & \
                   ingesta_time[2] >= last_datetime_time[2]:
                    print('ingesta.apend:',ingesta)
                    datetimes_list.append(ingesta)
                    print('datetimes_list:', datetimes_list)
                print('datetimes_list:', datetimes_list)
            print('datetimes_list:', datetimes_list)
            print(ingesta_date[0] >= last_datetime_date[0])
            print(ingesta_date[1] >= last_datetime_date[1])
            print(ingesta_date[2] >= last_datetime_date[2])
            print(ingesta_time[0] >= last_datetime_time[0])
            print(ingesta_time[1] >= last_datetime_time[1])
            print(ingesta_time[2] >= last_datetime_time[2])


        
        exit(0)
        # newDf = (self.sparkSession.spark.read.parquet(f'{self.pathNewData}/{tabela}/{ingestas[0]}'))
        # newDf.show()
        # query = f"SELECT * FROM {tabela} WHERE criacao > '{lastDate}' OR atualizacao > '{lastDate}'"
        # print('query:', query)
        # # SELECT * FROM vendas WHERE criacao > "2025-03-09 19:37:19.931771" OR atualizacao > "2025-03-09 19:37:19.931771"
        # try:
        #     return (self.sparkSession.spark.read
        #             .format("jdbc")
        #             .option("driver","com.mysql.cj.jdbc.Driver")
        #             .option("url", "jdbc:mysql://localhost:3306/vendas?allowPublicKeyRetrieval=true&useSSL=false")
        #             .option("query", query)
        #             .option("user", sys.argv[3])
        #             .option("password", sys.argv[4])
        #             .option("numPartitions", maxConn)
        #             .option("partitionsColumn", self.actors_id[tabela])
        #             .load()
        #         )
        # except Exception as e:
        #     print('Erro:', f'Não foi possível se conectar ao MYSQL - tabela "{tabela}"\n"{e}"')
        #     return None

    def SaveData(self, df, tabela):
        valid_columns = list(set(df.columns) & set(['atualizacao', 'criacao']))
        print('valid_columns:', valid_columns)
        df.show(5, truncate=False)
        last_datetime = (df
            .select(valid_columns)
            .withColumn('date', F.coalesce(valid_columns[0], valid_columns[-1]).cast('timestamp'))
            .drop(*valid_columns)
            .select(F.max(F.col('date')))
        ).collect()[0][0]
        print('last_datetime:', last_datetime)
        strTableDir = f'{self.pathData}/{tabela}'
        try:
            df.write.mode("overwrite").parquet(strTableDir)
            self.notificador.Mostrar('info', f'Master data set "{tabela}" salvo com sucesso')
            return 0
        except Exception as e:
            self.notificador.Mostrar('info', f'Master data set "{tabela}" com erro ao salvar - {e}')
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
        print('INGESTA')
        # return
        tabelas = list(self.actors_id.keys())
        for tabela in tabelas:
        # for tabela in tabelas[4:5]:
            print('Lendo tabela:', tabela)
            last_date = self.GetLastIngestaDatetime(tabela)
            print('last_date:', last_date)
            pathList = self.GetNewData(tabela, last_date)
            if pathList.count == 0:
                continue
            self.SaveData(pathList, tabela)
        print('FIM')
        # if results:
        #     columns=['MAX_CONN', 'QTD', 'BYTES POR LINHA']
        #     df = self.sparkSession.spark.createDataFrame(results,columns)
        #     df.show()