from sqlalchemy import text

def get_block_components_sql():
    """
    divides data on the fin blocks and adding component's data

    :return: sql query text
    """
    return text("""
    WITH fin_blocks AS (
        SELECT
            plant_id,
            produced_material AS fin_material_id,
            produced_material_release_type AS fin_rel_type,
            produced_material_production_type AS fin_prod_type,
            produced_material_quantity AS fin_quant,
            year,
            row_index AS start_index,
            LEAD(row_index) OVER (PARTITION BY plant_id, year ORDER BY row_index) AS end_index
        FROM production_data
        WHERE produced_material_release_type = 'FIN'
    )
    SELECT
        b.plant_id AS plant,
        b.fin_material_id,
        b.fin_rel_type,
        b.fin_prod_type,
        b.fin_quant,
        p.produced_material AS prod_material_id,
        p.produced_material_release_type AS prod_rel_type,
        p.produced_material_production_type AS prod_prod_type,
        p.produced_material_quantity AS prod_quant,
        p.component_material AS component_id,
        p.component_material_release_type AS comp_rel_type,
        p.component_material_production_type AS comp_prod_type,
        p.component_material_quantity AS comp_quant,
        p.year
    FROM fin_blocks b
    JOIN production_data p
        ON p.row_index >= b.start_index
        AND (b.end_index IS NULL OR p.row_index < b.end_index)
        AND p.plant_id = b.plant_id
        AND p.year = b.year
    ORDER BY b.plant_id, b.year, b.fin_material_id, p.row_index, p.component_material
    """)

def build_bom(conn):
    """
    creating ready data rows using sql query

    :param conn: sqlalchemy connection
    :return: ready data rows
    """
    sql = get_block_components_sql()
    result = conn.execute(sql)
    rows = []

    for row in result.mappings():
        if row["prod_material_id"] == row["fin_material_id"]:
            continue

        rows.append({
            "plant": row["plant"],
            "fin_mat_id": row["fin_material_id"],
            "fin_mat_rel_type": row["fin_rel_type"],
            "fin_mat_prod_type": row["fin_prod_type"],
            "fin_prod_quant": row["fin_quant"],
            "prod_mat_id": row["prod_material_id"],
            "prod_mat_rel_type": row["prod_rel_type"],
            "prod_mat_prod_type": row["prod_prod_type"],
            "prod_mat_quant": row["prod_quant"],
            "comp_mat_id": row["component_id"],
            "comp_mat_rel_type": row["comp_rel_type"],
            "comp_mat_prod_type": row["comp_prod_type"],
            "comp_mat_quant": row["comp_quant"],
            "year": row["year"]
        })

    return rows
