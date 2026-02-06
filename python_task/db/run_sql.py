from pathlib import Path


def run_sql_file(cursor, sql_file: Path):
    sql = sql_file.read_text(encoding="utf-8")
    cursor.execute(sql)
