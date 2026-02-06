def find_components(data, i, produced_material):
    """
    function for finding components for any produced_material

    :param data: input pandas table
    :param i: number of the produced_material string
    :param produced_material: integer from the table in the colomn produced_material
    :return: 4 lists with components info(id, rel_type, prod_type, cons_quant)
    """
    j = i
    comp_ids = []
    comp_mat_rel_types = []
    comp_mat_prod_types = []
    comp_cons_quants = []
    while j < len(data) and (data["produced_material_release_type"][j] != "FIN" or j == i):
        if data["produced_material"][j] == produced_material:
            comp_ids.append(data["component_material"][j])
            comp_mat_rel_types.append(data["component_material_release_type"][j])
            comp_mat_prod_types.append(data["component_material_production_type"][j])
            comp_cons_quants.append(data["component_material_quantity"][j])
        j += 1
    return comp_ids, comp_mat_rel_types, comp_mat_prod_types, comp_cons_quants


def build_table_rec(data, i, plant, fin_mat_id, fin_mat_rel_type, fin_mat_prod_type, fin_prod_quant, \
                    year, produced_material, results_by_year):
    """
    recursive function to build 1 result block(rows for 1 final material)

    :param data: input pandas table
    :param i: number of the produced_material string
    :param plant, fin_mat_id, fin_mat_rel_type, fin_mat_prod_type, fin_prod_quant, year: final material info
    :param produced_material: integer from the table in the colomn produced_material
    :param results_by_year: output map, year is key
    :return: recursive function
    """

    prod_mat_ids, prod_mat_rel_types, prod_mat_prod_types, prod_mat_prod_quants = find_components(data, \
                                                                                                  i, produced_material)

    for j in range(len(prod_mat_ids)):
        comp_mat_ids, comp_mat_rel_types, comp_mat_prod_types, comp_mat_prod_quants = find_components(data, i, \
                                                                                                      prod_mat_ids[j])
        for k in range(len(comp_mat_ids)):
            row = [plant, fin_mat_id, fin_mat_rel_type, fin_mat_prod_type, fin_prod_quant, prod_mat_ids[j], \
                   prod_mat_rel_types[j], prod_mat_prod_types[j], prod_mat_prod_quants[j], comp_mat_ids[k], \
                   comp_mat_rel_types[k], comp_mat_prod_types[k], comp_mat_prod_quants[k], year]
            results_by_year.setdefault(year, []).append(row)
        if prod_mat_rel_types[j] == "PROD":
            build_table_rec(data, i, plant, fin_mat_id, fin_mat_rel_type, fin_mat_prod_type, fin_prod_quant, \
                            year, prod_mat_ids[j], results_by_year)