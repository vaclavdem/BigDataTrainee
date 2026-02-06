import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    user="postgres",
    password="123456",
    dbname="postgres"
)

cur = conn.cursor()