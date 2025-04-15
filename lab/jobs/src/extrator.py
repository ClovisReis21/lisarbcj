import sys, os, findspark, mysql.connector, shutil
import pyspark.sql.functions as F
from src.notificador import Notificador
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType

findspark.add_packages('mysql:mysql-connector-java:8.0.11')

class Extrator:
    def __init__(self, sparkSession):
        self.sparkSession = sparkSession
        self.baseDatetime = '2025-03-01 00:00:00.000'
        self.cleanTime = 30
        self.actors_id = {
            "vendedores": "id_vendedor",
            "vendas": "id_venda",
            "produtos": "id_produto",
            "clientes": "id_cliente",
            "itens_venda": "id_venda"
        }
        self.pathNewData = './lab/jobs/new_data'
                            # /home/cj/lisarb_jc/lab/jobs/new_data/vendas
        self.pathNewDataMap = f'{self.pathNewData}/new_data_map'
        self.notificador = Notificador()

    def GetLastIngestaDatetime(self, tabela):
        fullPath = f'{self.pathNewData}/{tabela}'
        temp_df = None
        try:
            temp_df = (self.sparkSession.spark.read.parquet(self.pathNewDataMap)
                       .where(F.col("table") == tabela)
                       .sort("date", ascending=False))
            row = temp_df.collect()[0]
            return f'{row[1]}'
        except Exception as e:
            self.notificador.Mostrar('info', f'Datetime considerado "{self.baseDatetime}" para "{fullPath}" - {e}')
            return self.baseDatetime
    
    def SetLastIngestaDatetime(self, tabela, last_date):
        path_data = f'{self.pathNewData}/{tabela}/{last_date}'
        newData_df = self.sparkSession.spark.createDataFrame(
            [(tabela, last_date, path_data,)],
            schema=StructType([
                StructField("table", StringType(), False),
                StructField("date", StringType(), False),
                StructField("path", StringType(), False)
            ])
        )
        newData_df.write.mode("append").parquet(self.pathNewDataMap)

    def GetUserInfo(self, lastDate, tabela): # Read from MySQL procedure
        sql_host=sys.argv[2]
        sql_user=sys.argv[3]
        sql_password=sys.argv[4]
        sql_database=sys.argv[5]
        results = None
        try:
            conn = mysql.connector.connect(
                host=sql_host,
                user=sql_user,
                password=sql_password,
                database=sql_database
            )
            cursor=conn.cursor()
            cursor.callproc('USER_INFO', [lastDate, tabela])
            for result in cursor.stored_results():
                results = result.fetchall()
            cursor.close()
            conn.close()
        except Exception as e:
            self.notificador.Mostrar('error', f'Não foi possível se conectar ao MYSQL - tabela "{tabela}"\n"{e}"')
            return None
        self.notificador.Mostrar('info', f'USER_INFO: Max conn: {results[0][0]} | Low bound: {results[0][1]} | Upper bound: {results[0][2]} | Linhas: {results[0][3]}')
        return results

    def GetData(self, maxConn, lowBound, upperBound, lastDate, tabela): # Read from MySQL Table
        query = ""
        if tabela in ['itens_venda', 'vendas']:
            query = f"(SELECT * FROM {tabela} WHERE criacao > '{lastDate}') as subq"
        else:
            query = f"(SELECT * FROM {tabela} WHERE criacao > '{lastDate}' OR atualizacao > '{lastDate}') as subq"
        try:
            return (self.sparkSession.spark.read
                    .format("jdbc")
                    .option("driver","com.mysql.cj.jdbc.Driver")
                    .option("url", "jdbc:mysql://localhost:3306/vendas?allowPublicKeyRetrieval=true&useSSL=false")
                    .option("dbtable", query)
                    .option("user", sys.argv[3])
                    .option("password", sys.argv[4])
                    .option("numPartitions", maxConn)
                    .option("partitionColumn", self.actors_id[tabela])
                    .option("lowerBound", lowBound)
                    .option("upperBound", upperBound)
                    .load()
                )
        except Exception as e:
            self.notificador.Mostrar('error', f'Não foi possível se conectar ao MYSQL - tabela "{tabela}"\n"{e}"')
            return None

    def SaveData(self, df, tabela):
        valid_columns = list(set(df.columns) & set(['atualizacao', 'criacao']))
        last_datetime = (df
            .select(valid_columns)
            .withColumn('date', F.coalesce(valid_columns[0], valid_columns[-1]).cast('timestamp'))
            .drop(*valid_columns)
            .select(F.max(F.col('date')))
        ).collect()[0][0]
        strTableDir = f'{self.pathNewData}/{tabela}/{last_datetime}'
        try:
            df.write.mode("overwrite").parquet(strTableDir)
            self.notificador.Mostrar('info', f'"{strTableDir}" salvo com sucesso!')
            return f'{last_datetime}'
        except Exception as e:
            self.notificador.Mostrar('error', f'Não foi possível salvar "{strTableDir}" -> {e}')
            return self.baseDatetime
    
    def DropData(self):
        newDataMap_keep_df = (
            self.sparkSession.spark.read.parquet(f'{self.pathNewDataMap}')
                .where(
                    (F.to_timestamp(F.col('date')).alias('date') > (F.current_date() - self.cleanTime).alias('date')))
                .sort('date'))

        newDataMap_delete_df = (
            self.sparkSession.spark.read.parquet(f'{self.pathNewDataMap}')
                .where(
                    (F.to_timestamp(F.col('date')).alias('date') <= (F.current_date() - self.cleanTime).alias('date')))
                .sort('date'))

        pathsToDelete = newDataMap_delete_df.collect()
        try:
            for pathToDelete in pathsToDelete:
                shutil.rmtree(pathToDelete['path'])

            newDataMap_keep_df.write.mode("overwrite").parquet(f'{self.pathNewDataMap}')

        except Exception as e:
            self.notificador.Mostrar('info', f'Erro ao limpar MAP TABLE - {e}')

    def Run(self):
        tabelas = list(self.actors_id.keys())
        for tabela in tabelas:
            self.notificador.Mostrar('info', f'Iniciando extração da tabela "{tabela}"')
            last_date = self.GetLastIngestaDatetime(tabela)
            userInfo = self.GetUserInfo(last_date, tabela)
            QTD_LINHAS=userInfo[0][3]
            if QTD_LINHAS == 0:
                self.notificador.Mostrar('info', f'Não ha novos registros em "{tabela}"\n')
                continue
            MAX_CONN=userInfo[0][0]
            LOW_BOUND=userInfo[0][1]
            UPPER_BOUND=userInfo[0][2]
            df = self.GetData(MAX_CONN, LOW_BOUND, UPPER_BOUND, last_date, tabela)
            if df == None:
                self.notificador.Mostrar('error', f'Registros não encontrados em "{tabela}"\n')
                continue
            last_date = self.SaveData(df, tabela)
            self.SetLastIngestaDatetime(tabela, last_date)
            self.notificador.Mostrar('info', f'Extração da tabela "{tabela}" concluída!\n')
        baseNewDataMap = self.pathNewDataMap.split("/")[-1]
        # self.notificador.Mostrar('info', f'Iniciando atualização da base "{baseNewDataMap}"!')
        # self.DropData()
        # self.notificador.Mostrar('info', f'Base "{baseNewDataMap}" atualizada!\n')