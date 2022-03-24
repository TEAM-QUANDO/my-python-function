import os
from azure.storage.blob import BlobServiceClient


def upload_data_to_adls():
    """
    Function to upload local directory to ADLS
    :return:
    """
    # Azure Storage connection string
    connect_str = ""
    # Name of the Azure container
    container_name = ""
    # The path to be removed from the local directory path while uploading it to ADLS
    path_to_remove = ""
    # The local directory to upload to ADLS
    local_path = ""

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    # The below code block will iteratively traverse through the files and directories under the given folder and uploads to ADLS.
    for r, d, f in os.walk(local_path):
        if f:
            for file in f:
                file_path_on_azure = os.path.join(
                    r, file).replace(path_to_remove, "")
                file_path_on_local = os.path.join(r, file)
                blob_client = blob_service_client.get_blob_client(
                    container=container_name, blob=file_path_on_azure)
                with open(file_path_on_local, "rb") as data:
                    blob_client.upload_blob(data)
                    print("uploading file ---->", file_path_on_local)


class DownloadADLS:
    def __init__(self, connection_string, container_name):
        service_client = BlobServiceClient.from_connection_string(
            connection_string)
        self.client = service_client.get_container_client(container_name)

    def download(self, source, dest):
        '''
        Download a file or directory to a path on the local filesystem
        '''
        if not dest:
            raise Exception('A destination must be provided')

        blobs = self.ls_files(source, recursive=True)
        if blobs:
            # if source is a directory, dest must also be a directory
            if not source == '' and not source.endswith('/'):
                source += '/'
            if not dest.endswith('/'):
                dest += '/'
            # append the directory name from source to the destination
            dest += os.path.basename(os.path.normpath(source)) + '/'

            blobs = [source + blob for blob in blobs]
            for blob in blobs:
                blob_dest = dest + os.path.relpath(blob, source)
                self.download_file(blob, blob_dest)
        else:
            self.download_file(source, dest)

    def download_file(self, source, dest):
        '''
        Download a single file to a path on the local filesystem
        '''
        # dest is a directory if ending with '/' or '.', otherwise it's a file
        if dest.endswith('.'):
            dest += '/'
        blob_dest = dest + \
            os.path.basename(source) if dest.endswith('/') else dest

        print(f'Downloading {source} to {blob_dest}')
        os.makedirs(os.path.dirname(blob_dest), exist_ok=True)
        bc = self.client.get_blob_client(blob=source)
        with open(blob_dest, 'wb') as file:
            data = bc.download_blob()
            file.write(data.readall())

    def ls_files(self, path, recursive=False):
        '''
        List files under a path, optionally recursively
        '''
        if not path == '' and not path.endswith('/'):
            path += '/'

        blob_iter = self.client.list_blobs(name_starts_with=path)
        files = []
        for blob in blob_iter:
            relative_path = os.path.relpath(blob.name, path)
            if recursive or not '/' in relative_path:
                files.append(relative_path)
        return files

    def ls_dirs(self, path, recursive=False):
        '''
        List directories under a path, optionally recursively
        '''
        if not path == '' and not path.endswith('/'):
            path += '/'

        blob_iter = self.client.list_blobs(name_starts_with=path)
        dirs = []
        for blob in blob_iter:
            relative_dir = os.path.dirname(os.path.relpath(blob.name, path))
            if relative_dir and (recursive or not '/' in relative_dir) and not relative_dir in dirs:
                dirs.append(relative_dir)
        return dirs


if __name__ == '__main__':
    CONNECTION_STRING = ""
    CONTAINER_NAME = ""
    client = DownloadADLS(CONNECTION_STRING, CONTAINER_NAME)
    client.download(source="", dest="")
