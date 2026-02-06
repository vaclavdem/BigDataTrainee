from pathlib import Path
from db.connection import conn, cur
from data_io.export_json import export_sql_to_json

sql_dir = Path("sql")
output_dir = Path("output_files")
output_dir.mkdir(exist_ok=True)

for sql_file in sql_dir.glob("*.sql"):
    export_sql_to_json(cur, sql_file, output_dir)

conn.commit()
cur.close()
conn.close()