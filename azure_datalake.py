import os
import json
import re
import numpy as np
import pandas as pd
from datetime import datetime
from io import StringIO
from pypika import Query, Field, Column, Criterion
from pypika import Table as pTable
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake import DelimitedTextDialect, DelimitedJsonDialect


class AzureDatalake():
    def __init__(self, account_name: str, account_key: str, container_name: str):
        self.conn = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=account_key
        )
        self.container = self.conn.get_file_system_client(container_name)

    def blob_exists(self, filepath: str) -> bool:
        filepath = filepath.replace(os.sep, "/")
        file_client = self.container.get_file_client(filepath)
        return file_client.exists()

    def write_binary(self, outfile: str, infile: str):
        # extract filename and directory to be saved the cloud
        path = outfile.replace(os.sep, "/").split("/")[:-1]
        filepath_to_store_to = os.path.join(*path) if len(path) else "./"
        filename = os.path.normpath(outfile).split(os.sep)[-1]

        # create or get directory client
        directory_client = self.container.get_directory_client(
            filepath_to_store_to)

        # upload file
        file_client = directory_client.create_file(filename)
        with open(infile, "rb") as data:
            size = os.fstat(data.fileno()).st_size
            file_client.append_data(data, offset=0, length=size)

        # flush data once the process is completed
        return file_client.flush_data(size)

    def write_bytes(self, outfile: str, memstring: str, metadata=None):
        # extract filename and directory to be saved the cloud
        path = outfile.replace(os.sep, "/").split("/")[:-1]
        filepath_to_store_to = os.path.join(*path) if len(path) else "./"
        filename = os.path.normpath(outfile).split(os.sep)[-1]

        # create or get directory client
        directory_client = self.container.get_directory_client(
            filepath_to_store_to)

        # upload file
        file_client = directory_client.create_file(filename)
        file_client.append_data(data=memstring, offset=0,
                                length=len(memstring.encode("utf-8")))

        # flush data once the process is completed
        return file_client.flush_data(len(memstring.encode("utf-8")))

    def write_dataframe(self, filepath: str, data: pd.DataFrame, append=False):
        file_client = self.container.get_file_client(
            filepath.replace(os.sep, "/"))
        data = self.__normalize_timestamp__(data)
        if append:
            data: str = data.to_csv(index=False, header=False)
            # offset size must be known
            offset = file_client.get_file_properties()['size']
            file_client.append_data(data, offset)

            # upload data to cloud
            return file_client.flush_data(offset + len(data.encode("utf-8")))
        else:
            data = data.to_csv(index=False, header=True)
            file_client.create_file()
            return self.write_bytes(filepath, data)

    def read_bytes(self, filepath: str) -> bytes:
        file_client = self.container.get_file_client(
            filepath.replace(os.sep, "/"))
        return file_client.download_file().readall()

    def read_image(self, filepath: str) -> np.ndarray:
        data = self.read_bytes(filepath)
        return np.frombuffer(data, np.uint8)

    def read_dataframe(self, filepath, sep=',', engine='python', index_col=None, parse_dates=None) -> pd.DataFrame:
        data = self.read_bytes(filepath)
        if isinstance(data, (bytes, bytearray)):
            data = data.decode()

        return pd.read_csv(StringIO(data), sep=sep, engine=engine, index_col=index_col, parse_dates=parse_dates)

    def query_csv(self, sql_query: str, filepath: str):
        file_client = self.container.get_file_client(
            filepath.replace(os.sep, "/"))

        # setup formatter
        input_format = DelimitedTextDialect(
            delimiter=',', quotechar='"', escapechar="", has_header=True)
        output_format = DelimitedJsonDialect(delimiter=',')

        # query the data, then convert it to json object
        reader = file_client.query_file(
            sql_query, file_format=input_format, output_format=output_format)
        content = reader.readall()
        content = json.loads('[{0}]'.format(content.decode()[:-1]))
        return content

    def __normalize_timestamp__(self, df: pd.DataFrame) -> pd.DataFrame:
        dt_columns = df.select_dtypes('datetimetz')
        if len(dt_columns):
            df[dt_columns.columns] = dt_columns.apply(
                lambda x: x.dt.tz_convert('utc'))
            df[dt_columns.columns] = dt_columns.apply(
                lambda x: x.dt.strftime('%Y-%m-%dT%H:%M:%S+00:00'))

        dt_columns = df.select_dtypes('datetime64')
        if len(dt_columns):
            df[dt_columns.columns] = dt_columns.apply(
                lambda x: pd.to_datetime(x, utc=True))
            df[dt_columns.columns] = dt_columns.apply(
                lambda x: x.dt.strftime('%Y-%m-%dT%H:%M:%S+00:00'))
        return df


'''
def query_video_analyzer_room_time_range(client, filename, start, end=None):
    start = start.tz_convert("UTC")
    end = start if end is None else end.tz_convert("UTC")
    sql_expr = QueryBuilder("create_time").timestamp_between(start, end)
    data = client.query_log_to_json(sql_expr, filename)
    return data

def query_video_analyzer_participant_time_range(client, filename, start, end=None):
    start = start.tz_convert("UTC")
    end = start if end is None else end.tz_convert("UTC")
    sql_expr = QueryBuilder("join_time").timestamp_between(start, end)
    data = client.query_log_to_json(sql_expr, filename)
    return data

def query_video_analyzer_callhistory_time_range(client, filename, start, end=None):
    start = start.tz_convert("UTC")
    end = start if end is None else end.tz_convert("UTC")
    sql_expr = QueryBuilder("create_time").timestamp_between(start, end)
    data = client.query_log_to_json(sql_expr, filename)
    return data

def query_video_analyzer_participant_info_time_range(client, filename, start, end=None):
    start = start.tz_convert("UTC")
    end = start if end is None else end.tz_convert("UTC")
    sql_expr = QueryBuilder("join_time").timestamp_between(start, end)
    data = client.query_log_to_json(sql_expr, filename)
    return data
'''


class QueryBuilder(str):
    def __init__(self, value="*"):
        pass

    def __new__(cls, value="*"):
        if value == "*":
            value = "SELECT * FROM BlobStorage"
        return super().__new__(cls, value)

    def timestamp_between(self, start, end, selected_vars="*"):
        # filter condition
        after = self.__to_timestamp(start)
        before = end + datetime.timedelta(days=1)
        before = self.__to_timestamp(before)

        # build SQL query expression
        table = pTable("BlobStorage")
        selected_vars = self.__parse_select_vars(selected_vars)
        query = Query.from_(table).select(*selected_vars).where(
            (table[self.__as_timestamp()] > Field(after)) &
            (table[self.__as_timestamp()] < Field(before))
        ).get_sql()
        return self.__parse_str(query)

    def timestamp_after(self, after, selected_vars="*"):
        table = pTable("BlobStorage")
        selected_vars = self.__parse_select_vars(selected_vars)
        after = self.__to_timestamp(after)
        query = Query.from_(table).select(*selected_vars).where(
            table[self.__as_timestamp()] > Field(after)
        ).get_sql()
        return self.__parse_str(query)

    def timestamp_before(self, before, selected_vars="*"):
        table = pTable("BlobStorage")
        before = self.__to_timestamp(before)
        selected_vars = self.__parse_select_vars(selected_vars)
        query = Query.from_(table).select(*selected_vars).where(
            table[self.__as_timestamp()] < Field(before)
        ).get_sql()
        return self.__parse_str(query)

    def between(self, lower, upper, selected_vars="*"):
        table = pTable("BlobStorage")
        selected_vars = self.__parse_select_vars(selected_vars)
        query = Query.from_(table).select(*selected_vars).where(
            (table[self] > lower) &
            (table[self] <= upper)
        ).get_sql()
        return self.__parse_str(query)

    def gt(self, value, selected_vars="*"):
        table = pTable("BlobStorage")
        selected_vars = self.__parse_select_vars(selected_vars)
        query = Query.from_(table).select(*selected_vars).where(
            (table[self] > value)
        ).get_sql()
        return self.__parse_str(query)

    def lt(self, value, selected_vars="*"):
        table = pTable("BlobStorage")
        selected_vars = self.__parse_select_vars(selected_vars)
        query = Query.from_(table).select(*selected_vars).where(
            (table[self] < value)
        ).get_sql()
        return self.__parse_str(query)

    def ge(self, value, selected_vars="*"):
        table = pTable("BlobStorage")
        selected_vars = self.__parse_select_vars(selected_vars)
        query = Query.from_(table).select(*selected_vars).where(
            (table[self] >= value)
        ).get_sql()
        return self.__parse_str(query)

    def le(self, value, selected_vars="*"):
        table = pTable("BlobStorage")
        selected_vars = self.__parse_select_vars(selected_vars)
        query = Query.from_(table).select(*selected_vars).where(
            (table[self] <= value)
        ).get_sql()
        return self.__parse_str(query)

    def isin(self, having, selected_vars="*"):
        table = pTable('BlobStorage')
        selected_vars = self.__parse_select_vars(selected_vars)
        query = Query.from_(table).select(*selected_vars).where(
            table[self]
        ).isin(having).get_sql()
        return self.__parse_str(query)

    def contains(self, contain_list, selected_vars="*"):
        table = pTable("BlobStorage")
        selected_vars = self.__parse_select_vars(selected_vars)
        query = Query.from_(table).select(*selected_vars).where(
            table[self]
        ).like(contain_list).get_sql()

        # index position of WHERE statement
        WHERE_index = query.find("WHERE")

        # grab column name (find open and close quotation mark)
        open_x = WHERE_index + 6
        close_x = (open_x + 1) + query[open_x + 1:].find("\"") + 1
        colname = query[open_x:close_x]

        # list of matching list
        open_x = query.find("[")
        close_x = (open_x + 1) + query[open_x + 1:].find("]") + 1
        contain_list = query[open_x + 1:close_x - 1].split(",")

        # foramt matching list into statement
        contain_expr = []
        for contain in contain_list:
            contain_expr.append("{0} LIKE {1}".format(colname, contain))
        query = "{}".format(query[:WHERE_index + len("WHERE") + 1])

        # rewrite query expression
        for item in contain_expr:
            query += "{} OR ".format(item)

        # remove "OR " at the end
        query = query[:-4]
        return self.__parse_str(query)

    def all(self, condition_list, selected_vars="*"):
        table = pTable("BlobStorage")
        locs = locals()
        selected_vars = self.__parse_select_vars(selected_vars)
        query = Query.from_(table).select(*selected_vars).where(
            Criterion.all([
                eval(condition, locs) for condition in condition_list
            ])
        ).get_sql()
        query = self.__parse_timestamp(query)
        return self.__parse_str(query)

    def any(self, condition_list, selected_vars="*"):
        table = pTable("BlobStorage")
        locs = locals()
        selected_vars = self.__parse_select_vars(selected_vars)
        query = Query.from_(table).select(*selected_vars).where(
            Criterion.any([
                eval(condition, locs) for condition in condition_list
            ])
        ).get_sql()
        query = self.__parse_timestamp(query)
        return self.__parse_str(query)

    def __parse_timestamp(self, expr):
        regex = re.compile(r'[0-9]*-[0-9][0-9]-[0-9][0-9]', re.S)
        expr = regex.sub(lambda m: pd.to_datetime(m.group()).strftime(
            "TO_TIMESTAMP(\'%Y-%m-%dT%H:%M:%S+00:00\')"), expr)
        regex = re.compile(r'\'TO_TIMESTAMP', re.S)
        expr = regex.sub(lambda m: m.group().replace(
            '\'TO_TIMESTAMP', "TO_TIMESTAMP"), expr)
        expr = re.sub(r'\)\'', ")", expr)
        return expr

    def __parse_select_vars(self, selected_vars):
        if not isinstance(selected_vars, str):
            selected_vars = [Column(item) for item in selected_vars]
        else:
            selected_vars = [Column(item) for item in ["*"]]
        return selected_vars

    def __to_timestamp(self, datetime, timezone="+00:00"):
        # if timezone information is not specified, it is assumed to be UTC
        if datetime.tzinfo is None:
            return "TO_TIMESTAMP('{0}{1}')".format(datetime.isoformat(), timezone)
        else:
            return "TO_TIMESTAMP('{}')".format(datetime.isoformat())

    def __as_timestamp(self):
        return "CAST(\"{0}\" AS TIMESTAMP)".format(self)

    def __parse_str(self, sql_expr):
        regex = re.compile('"[A-Z_]*\(', re.S)
        sql_expr = regex.sub(lambda m: m.group().replace('"', ""), sql_expr)
        regex = re.compile('[A-Z]*\)"', re.S)
        sql_expr = regex.sub(lambda m: m.group().replace('"', ""), sql_expr)

        regex = re.compile('[A-Z][A-Z]*\"', re.S)
        sql_expr = regex.sub(lambda m: m.group().replace('"', ""), sql_expr)

        # SELECT "*" -> SELECT *
        regex = re.compile('"\*"', re.S)
        sql_expr = regex.sub(lambda m: m.group().replace('"*"', "*"), sql_expr)

        return sql_expr
