import sys, os, findspark, mysql.connector
import pyspark.sql.functions as F
from src.notificador import Notificador

findspark.add_packages('mysql:mysql-connector-java:8.0.11')

class Extrator:
    def __init__(self, sparkSession):
        self.sparkSession = sparkSession
        self.firstDatetime = '2025-03-01 00:00:00.000'
        self.actors_id = {
            "vendedores": "id_vendedor",
            "vendas": "id_venda",
            "produtos": "id_produto",
            "clientes": "id_cliente",
            "itens_venda": "id_venda"
        }
        self.pathNewData = './lab/jobs/new_data'
        self.notificador = Notificador()

    def GetLastIngestaDatetime(self, tabela):
        fullPath = f'{self.pathNewData}/{tabela}'
        os.makedirs(fullPath, exist_ok=True)
        ingestas = os.listdir(fullPath)
        if len(ingestas) < 1:
            self.notificador.Mostrar('info', f'Datetime considerado: {self.firstDatetime} para: "{fullPath}"')
            return '2025-03-01 00:00:00.000'
        ingestas.sort(reverse=True)
        last_ingesta = ingestas[0]
        self.notificador.Mostrar('info', f'Datetime considerado: {last_ingesta} para: "{fullPath}"')
        return last_ingesta

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
        last_datetime = (df
            .select('atualizacao', 'criacao')
            .withColumn('date', F.coalesce(F.col('atualizacao'),F.col('criacao')).cast('timestamp'))
            .drop('atualizacao', 'criacao')
            .select(F.max(F.col('date')))
        ).collect()[0][0]
        strTableDir = f'{self.pathNewData}/{tabela}/{last_datetime}'
        try:
            df.write.mode("overwrite").parquet(strTableDir)
            self.notificador.Mostrar('info', f'"{strTableDir}" salvo com sucesso!')
            return 0
        except Exception as e:
            self.notificador.Mostrar('error', f'Não foi possível salvar: "{strTableDir}" -> {e}')
            return 1

    def Run(self):
        tabelas = list(self.actors_id.keys())
        for tabela in tabelas:
            self.notificador.Mostrar('info', f'Iniciando extração da tabela: "{tabela}"')
            last_date = self.GetLastIngestaDatetime(tabela)
            userInfo = self.GetUserInfo(last_date, tabela)
            QTD_LINHAS=userInfo[0][3]
            if QTD_LINHAS == 0:
                self.notificador.Mostrar('info', f'Não ha novos registros em: {tabela}')
                continue
            MAX_CONN=userInfo[0][0]
            LOW_BOUND=userInfo[0][1]
            UPPER_BOUND=userInfo[0][2]
            print(MAX_CONN,LOW_BOUND,UPPER_BOUND)
            df = self.GetData(MAX_CONN, LOW_BOUND, UPPER_BOUND, last_date, tabela)
            if df == None:
                self.notificador.Mostrar('error', f'Registros não encontrados em: {tabela}')
                continue
            df.show(1)
            self.SaveData(df, tabela)
            self.notificador.Mostrar('info', f'Extração da tabela: "{tabela}" concluída!\n')