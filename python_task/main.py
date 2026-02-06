import argparse
from pathlib import Path
from db.connection import conn, cursor
from data_io.load_data import insert_rooms, insert_students
from data_io.export_json import export_sql_to_json
from data_io.import_json import import_rooms_info, import_students_info
from db.run_sql import run_sql_file


def parse_args():
    parser = argparse.ArgumentParser(description="Rooms and students processing")
    parser.add_argument(
        "--rooms",
        required=True,
        help="Path to rooms JSON file"
    )
    parser.add_argument(
        "--students",
        required=True,
        help="Path to students JSON file"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    sql_dir = Path("sql")
    ddl_file = sql_dir / "ddl" / "schema.sql"
    output_dir = Path("output_files")
    output_dir.mkdir(exist_ok=True)

    rooms = import_rooms_info(cursor, args)
    students = import_students_info(cursor, args)

    run_sql_file(cursor, ddl_file)

    insert_rooms(cursor, rooms)
    insert_students(cursor, students)

    for sql_file in sql_dir.glob("*.sql"):
        export_sql_to_json(cursor, sql_file, output_dir)

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
