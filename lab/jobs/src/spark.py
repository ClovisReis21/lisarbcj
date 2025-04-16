from pyspark.sql import SparkSession

class Spark:
    def __init__(self, appName):
        self.spark = (SparkSession.builder
           .appName(appName)
           .config("packages", "org.apache.spark:mysql-connector-java-8.0.13.jar")
           .config("spark.master", "local[2]")
           .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("OFF")
        # return self.spark