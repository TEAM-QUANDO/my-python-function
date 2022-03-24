import os
import json
import pandas as pd


def data_extractor(file_path, delimiter, required_fields=[]):
    """
    :param file_path:
    :param delimiter:
    :param required_fields:
    :return:
    """
    if len(required_fields) > 0:
        df = pd.read_csv(file_path, sep=delimiter, usecols=required_fields)
    else:
        df = pd.read_csv(file_path, sep=delimiter)
    data_list = df.to_dict('records')
    print("Record Count --->", len(data_list))
    return data_list


def divide_chunks(l, n):
    """
    :param l: list
    :param n: number of splits
    :return: list of smaller lists
    """
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


def split_writer(list_of_lists, output_dir, file_prefix="data_"):
    """
    Function Description
    :param list_of_lists:
    :param output_dir:
    :param file_prefix:
    :return:
    """
    i = 0
    for each_list in list_of_lists:
        f = pd.DataFrame(each_list)
        data_prefix = os.path.join(output_dir, file_prefix)
        fw = open(data_prefix + str(i) + ".csv", "w", encoding='utf-8')
        fw.write(json.dumps(f))
        fw.close()
        i += 1
    print("Total number of file splits -->", i+1)


if __name__ == '__main__':
    file_path = 'large_data.csv'
    # specify the required fields to extract from the file.
    # You can keep this empty if you want to consider all the fields
    required_fields = []
    # specify the delimiter
    delimiter = "\t"
    # Number of records per file
    number_of_records_per_file = 2000
    # Output directory
    out_dir = "outdir"
    d_list = data_extractor(file_path, delimiter, required_fields)
    list_of_lists = list(divide_chunks(d_list, number_of_records_per_file))
    split_writer(list_of_lists, out_dir)
