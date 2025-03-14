import sys, os, findspark, datetime, mysql.connector
import pyspark.sql.functions as F

findspark.add_packages('mysql:mysql-connector-java:8.0.11')

class Extrator:
    def __init__(self, sparkSession):
        self.dir_path = os.getcwd()
        self.sparkSession = sparkSession
        self.actors_id = {
            "vendedores": "id_vendedor",
            "vendas": "id_venda",
            "produtos": "id_produto",
            "clientes": "id_cliente",
            "itens_venda": "id_venda"
        }
        self.pathNewData = './lab/jobs/new_data'

    def GetLastIngestaDatetime(self, tabela):
        fullPath = f'{self.pathNewData}/{tabela}'
        os.makedirs(fullPath, exist_ok=True)
        ingestas = os.listdir(fullPath)
        if len(ingestas) < 1:
            return '2025-03-01 00:00:00.000'
        ingestas.sort(reverse=True)
        last_ingesta = ingestas[0]
        for ingesta in ingestas:
            print(ingesta)
        print('last_ingesta:', last_ingesta)
        return last_ingesta

    def GetUserInfo(self, lastDate, tabela): # Read from MySQL procedure
        print(f'GetUserInfo({lastDate}, {tabela})')
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
            print('Erro:', f'Não foi possível se conectar ao MYSQL - tabela "{tabela}"\n"{e}"')
            return None
        return results

    def GetData(self, maxConn, lastDate, tabela): # Read from MySQL Table
        query = f"SELECT * FROM {tabela} WHERE criacao > '{lastDate}' OR atualizacao > '{lastDate}'"
        print('query:', query)
        # SELECT * FROM vendas WHERE criacao > "2025-03-09 19:37:19.931771" OR atualizacao > "2025-03-09 19:37:19.931771"
        try:
            return (self.sparkSession.spark.read
                    .format("jdbc")
                    .option("driver","com.mysql.cj.jdbc.Driver")
                    .option("url", "jdbc:mysql://localhost:3306/vendas?allowPublicKeyRetrieval=true&useSSL=false")
                    .option("query", query)
                    .option("user", sys.argv[3])
                    .option("password", sys.argv[4])
                    .option("numPartitions", maxConn)
                    .option("partitionsColumn", self.actors_id[tabela])
                    .load()
                )
        except Exception as e:
            print('Erro:', f'Não foi possível se conectar ao MYSQL - tabela "{tabela}"\n"{e}"')
            return None

    def SaveData(self, df, tabela):
        last_datetime = (df
            .select('atualizacao', 'criacao')
            .withColumn('date', F.coalesce(F.col('atualizacao'),F.col('criacao')).cast('timestamp'))
            .drop('atualizacao', 'criacao')
            .select(F.max(F.col('date')))
        ).collect()[0][0]
        print('last_datetime:', last_datetime)
        strTableDir = f'{self.pathNewData}/{tabela}/{last_datetime}'
        try:
            df.write.mode("overwrite").parquet(strTableDir)
            return 0
        except:
            return 1

    def Run(self):
        tabelas = list(self.actors_id.keys())
        for tabela in tabelas:
            print('Lendo tabela:', tabela)
            last_date = self.GetLastIngestaDatetime(tabela)
            print('last_date:', last_date[:23])
            userInfo = self.GetUserInfo(last_date, tabela)
            QTD=userInfo[0][1]
            if QTD == 0:
                continue
            MAX_CONN=userInfo[0][0]
            BYTES_POR_LINHA=userInfo[0][2]
            print(userInfo, MAX_CONN, QTD, BYTES_POR_LINHA, QTD * BYTES_POR_LINHA)
            df = self.GetData(MAX_CONN, last_date, tabela)
            df.show()
            self.SaveData(df, tabela)
        print('FIM')
        # if results:
        #     columns=['MAX_CONN', 'QTD', 'BYTES POR LINHA']
        #     df = self.sparkSession.spark.createDataFrame(results,columns)
        #     df.show()