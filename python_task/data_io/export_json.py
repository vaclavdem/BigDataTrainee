import json
from pathlib import Path
from datetime import timedelta


def serialize(value):
    """
    function to make our data json friendly

    :param value: value that we need to serialize
    :return: serialized value
    """
    if isinstance(value, timedelta):
        return value.days
    return value


def export_sql_to_json(cursor, sql_file: Path, output_dir: Path):
    """
    function to export result of sql query in json file

    :param cursor: cursor db
    :param sql_file: file with sql query
    :param output_dir: dir for output files
    :return: new or edited file in output dir
    """
    query = sql_file.read_text(encoding="utf-8")
    cursor.execute(query)

    rows = cursor.fetchall()
    normalized_rows = [
        [serialize(value) for value in row]
        for row in rows
    ]

    output_file = output_dir / f"{sql_file.stem}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(normalized_rows, f, ensure_ascii=False, indent=2)

    print(f"Exported {sql_file.name} -> {output_file.name}")
