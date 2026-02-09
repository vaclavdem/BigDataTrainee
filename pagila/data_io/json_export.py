file_path = "C:/Users/vatsl/PycharmProjects/pagila_spark/pagila/output_files/"

def saving_into_json(dataframe, name):
    """
    function to export dataframe to json file

    :param dataframe: dataframe to export
    :param name: output file name
    :return: json file
    """
    output_dir = file_path + name
    print(f"Saving to: {output_dir}")

    dataframe.coalesce(1).write \
      .format("json") \
      .mode("overwrite") \
      .save(file_path + name)