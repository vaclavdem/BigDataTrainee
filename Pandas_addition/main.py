import pandas as pd
from services.pandas_calculations import build_table_rec
from services.sql_calculations import build_bom
from db.connection import engine
from db.load_data import insert_data

data = pd.read_excel("input_files/task_2_data_ex.xlsx")

results_by_year = {}
columns = ["plant", "fin_mat_id", "fin_mat_rel_type", "fin_mat_prod_type", "fin_prod_quant", "prod_mat_id", \
    "prod_mat_rel_type", "prod_mat_prod_type", "prod_mat_quant", "comp_mat_id", "comp_mat_rel_type", \
    "comp_mat_prod_type", "comp_mat_quant", "year"]

# PANDAS CALCULATION

for i in range(len(data["year"])):
    if data["produced_material_release_type"][i] == "FIN":
        plant = data["plant_id"][i]
        fin_mat_id = data["produced_material"][i]
        fin_mat_rel_type = data["produced_material_release_type"][i]
        fin_mat_prod_type = data["produced_material_production_type"][i]
        fin_prod_quant = data["produced_material_quantity"][i]
        year = data["year"][i]
        build_table_rec(data, i, plant, fin_mat_id, fin_mat_rel_type, fin_mat_prod_type, fin_prod_quant,\
                    year, data["produced_material"][i], results_by_year)

with pd.ExcelWriter("output_files/result_pandas.xlsx", engine="xlsxwriter") as writer:
    for year, rows in results_by_year.items():
        df = pd.DataFrame(rows, columns=columns)
        df.to_excel(writer, sheet_name=str(year), index=False)


# SQL CALCULATION

insert_data(engine, data)

all_rows = []
with engine.connect() as conn:
    all_rows = build_bom(conn)

df = pd.DataFrame(all_rows, columns=columns)

with pd.ExcelWriter("output_files/result_sql.xlsx", engine="xlsxwriter") as writer:
    for year, df_year in df.groupby("year"):
        df_year.to_excel(writer, sheet_name=str(year), index=False)