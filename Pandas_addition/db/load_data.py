import pandas as pd
from sqlalchemy import text


def insert_data(engine, data: pd.DataFrame):
    """
    inserting data in posgresql table

    :param engine: sqlalchemy connection
    :param data: data from input excel file
    :return:
    """
    data = data.reset_index().rename(columns={"index": "row_index"})

    data.to_sql(
        name="production_data",
        con=engine,
        schema="public",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000
    )

    index_sql = [
        """CREATE INDEX IF NOT EXISTS idx_row_index
           ON production_data (row_index);""",
        """CREATE INDEX IF NOT EXISTS idx_prod_mat
           ON production_data (produced_material);""",
        """CREATE INDEX IF NOT EXISTS idx_comp_mat
           ON production_data (component_material);""",
        """CREATE INDEX IF NOT EXISTS idx_fin_type
           ON production_data (produced_material_release_type);"""
    ]

    with engine.begin() as conn:
        for stmt in index_sql:
            conn.execute(text(stmt))
