from db.spark_connection import spark

def jdbc_input_function(table):
    """
    function to load table from database

    :param table: table's name
    :return: loaded db table
    """
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/postgres") \
        .option("dbtable", table) \
        .option("user", "postgres") \
        .option("password", "123456") \
        .option("driver", "org.postgresql.Driver") \
        .load()