from pyspark.sql import SparkSession
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

class Spark:
    def __init__(self, appName):
        path = os.getcwd()
        print(f'Initializing Spark with appName: {appName} -> {path}')
        self.spark = (SparkSession.builder
           .appName(appName)
            .config("spark.jars", ",".join([
                f"{path}/src/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar",
                f"{path}/src/jars/nessie-spark-extensions-3.5_2.12-0.103.3.jar",
                f"{path}/src/jars/iceberg-nessie-1.5.0.jar",
                f"{path}/src/jars/mysql-connector-java-8.0.11.jar",
            ]))
            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
            .config("spark.sql.catalog.nessie.uri", "http://172.28.0.13:19120/api/v1")
            .config("spark.sql.catalog.nessie.ref", "main")
            .config("spark.sql.catalog.nessie.warehouse", "hdfs://172.28.0.14:8020/warehouse")
            
           .getOrCreate()
        )
    
    def get(self):
        return self.spark

    def stop(self):
        try:
            print("Stopping Spark session...")
            self.spark.stop()
            print("Spark session stopped successfully.")
        except Exception as e:
            print(f"Error stopping Spark session: {e}")
