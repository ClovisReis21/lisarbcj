from pyspark.sql import SparkSession

class Spark:
    def __init__(self):
        self.spark = (SparkSession.builder
           .appName('Ingesta')
           .config("packages", "org.apache.spark:mysql-connector-java-8.0.13.jar")
           .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("OFF")
        # return self.spark