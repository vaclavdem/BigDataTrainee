from db.spark_connection import spark
from dotenv import load_dotenv
import os

load_dotenv()

def jdbc_input_function(table):
    """
    function to load table from database

    :param table: table's name
    :return: loaded db table
    """
    return spark.read.format("jdbc") \
        .option("url", os.getenv("URL")) \
        .option("dbtable", table) \
        .option("user", os.getenv("USER")) \
        .option("password", os.getenv("PASSWORD")) \
        .option("driver", "org.postgresql.Driver") \
        .load()