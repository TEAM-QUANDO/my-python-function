import os
import json
import pandas as pd
from io import StringIO

from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake import DelimitedTextDialect, DelimitedJsonDialect

class AzureDatalakeV2():
    def __init__(self, account_name: str, account_key: str, container_name: str):
        self.conn = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=account_key
        )
        self.container = self.conn.get_file_system_client(container_name)

    def save_binary_file(self, path_to_store_to: str, path_to_local_file: str):
        # extract filename and directory to be saved the cloud
        path = path_to_store_to.replace(os.sep, "/").split("/")[:-1]
        filepath_to_store_to = os.path.join(*path) if len(path) else "./"
        filename = os.path.normpath(path_to_store_to).split(os.sep)[-1]

        # create or get directory client
        directory_client = self.container.get_directory_client(
            filepath_to_store_to)

        # upload file
        file_client = directory_client.create_file(filename)
        with open(path_to_local_file, "rb") as data:
            size = os.fstat(data.fileno()).st_size
            file_client.append_data(data, offset=0, length=size)

        # flush data once the process is completed
        return file_client.flush_data(size)

    def store_file_from_memory(self, path_to_store_to, memstring, metadata=None):
        # extract filename and directory to be saved the cloud
        path = path_to_store_to.replace("\\", "/").split("/")[:-1]
        filepath_to_store_to = os.path.join(*path) if len(path) else "./"
        filename = os.path.normpath(path_to_store_to).split(os.sep)[-1]

        # create or get directory client
        directory_client = self.container.get_directory_client(
            filepath_to_store_to)

        # upload file
        file_client = directory_client.create_file(filename)
        file_client.append_data(data=memstring, offset=0,
                                length=len(memstring.encode("utf-8")))

        # flush data once the process is completed
        return file_client.flush_data(len(memstring.encode("utf-8")))

    def save_dataframe(self, filepath: str, data: pd.DataFrame, overwrite=False):
        file_client = self.container.get_file_client(
            filepath.replace(os.sep, "/"))
        data = self.__normalize_timestamp__(data)
        if overwrite:
            data = data.to_csv(index=False, header=True)
            file_client.create_file()
            return self.store_file_from_memory(filepath, data)
        else:
            data = data.to_csv(index=False, header=False)
            # offset size must be known
            offset = file_client.get_file_properties()['size']
            file_client.append_data(data, offset)

            # upload data to cloud
            return file_client.flush_data(offset + len(data.encode("utf-8")))

    def read_dataframe_as_bytes(self, filepath: str) -> bytes:
        file_client = self.container.get_file_client(
            filepath.replace(os.sep, "/"))
        return file_client.download_file().readall()

    def read_dataframe(self, filepath, sep=',', engine='python', index_col=None, parse_dates=None):
        data = self.read_dataframe_as_bytes(filepath)
        if isinstance(data, (bytes, bytearray)):
            data = data.decode()

        return pd.read_csv(StringIO(data), sep=sep, engine=engine, index_col=index_col, parse_dates=parse_dates)

    def query_dataframe_as_json(self, sql_query: str, filepath: str):
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
        # convert to utc
        dt_columns = df.select_dtypes('datetimetz')
        df[dt_columns.columns] = dt_columns.apply(
            lambda x: x.dt.tz_convert('utc'))
        dt_columns = df.select_dtypes('datetime64')
        df[dt_columns.columns] = dt_columns.apply(
            lambda x: pd.to_datetime(x, utc=True))

        # it has to be in this format when query timestamp
        df[dt_columns.columns] = dt_columns.apply(
            lambda x: x.dt.strftime('%Y-%m-%dT%H:%M:%S+00:00'))
        return df