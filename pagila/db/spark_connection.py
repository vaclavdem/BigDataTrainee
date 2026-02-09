from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("pagila") \
    .config("spark.jars", "file:///C:/spark/jars/postgresql-42.7.9.jar") \
    .getOrCreate()