from azure.storage.blob import BlobServiceClient
from git import RemoteProgress
import shutil
import git
import uuid
import os
# Modify this path as per the execution environment
os.environ['GIT_PYTHON_GIT_EXECUTABLE'] = r"C:\Program Files\Git\bin\git.exe"


class CloneProgress(RemoteProgress):
    """
    This is to print the progress of the clone process
    """

    def update(self, op_code, cur_count, max_count=None, message=''):
        if message:
            print(message)


def clean_up_local(directory):
    """
    Function to clean up a directory in the local machine
    This function cleans up the local temporary directory
    :param directory:
    :return:
    """
    if os.path.exists(directory):
        shutil.rmtree(directory, ignore_errors=True)
        hidden_path = os.path.join(directory, '.git')
        if os.path.exists(hidden_path):
            shutil.rmtree('.unwanted', ignore_errors=True)
    try:
        os.mkdir(directory)
    except OSError as error:
        print(error)
        pass
    temp_path = str(uuid.uuid4())[0:6]
    return temp_path


def clone_git(git_url, git_branch, git_local_clone_path):
    """
    Function to clone the git repository to local disk.
    :return: status - True/False based on the status.
    """
    git.Repo.clone_from(git_url, git_local_clone_path,
                        branch=git_branch, progress=CloneProgress())
    return True


def delete_adls_directory(connect_str, container_name, prefix):
    """
    Function to delete a directory in ADLS
    :param connect_str:
    :param container_name:
    :param prefix:
    :return:
    """
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(
        container=container_name)
    blob_list = container_client.list_blobs(name_starts_with=prefix)
    new_blob_list = []
    for blob in blob_list:
        new_blob_list.append(str(blob.name))
    print("Length --->", len(new_blob_list), type(new_blob_list))
    for blb in reversed(new_blob_list):
        print("Deleting -->", blb)
        container_client.delete_blob(blb)


def upload_data_to_adls(connect_str, container_name, path_to_remove, local_path, target_base_path):
    """
    Function to upload local directory to ADLS
    :return:
    """
    print("ADLS Container Base Path --->", target_base_path)
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    for r, d, f in os.walk(local_path):
        if f:
            for file in f:
                file_path_on_azure = os.path.join(
                    r, file).replace(path_to_remove, "")
                file_path_on_azure = file_path_on_azure.lstrip(
                    "\\").lstrip('/')
                file_path_on_azure = os.path.join(
                    target_base_path, file_path_on_azure)
                print("File Path on Azure --------->", file_path_on_azure)
                print(file_path_on_azure)
                file_path_on_local = os.path.join(r, file)
                blob_client = blob_service_client.get_blob_client(
                    container=container_name, blob=file_path_on_azure)
                with open(file_path_on_local, "rb") as data:
                    blob_client.upload_blob(data)
                    print("Uploading file ---->", file_path_on_local)
                blob_client.close()


def main():
    # Git / Repos details
    # Git URL in the format --> https://username:password@giturl
    git_url = ""
    # Git branch
    git_branch = ""
    # Git project name. This will be the base folder name of the git project
    git_project_name = ""
    # Base path in the execution environment to store temporary data
    temp_base_path = "localtemp"
    # The relative directory of the git project to upload
    upload_src_directory = ""
    # Azure Storage account connection string
    connect_str = ""
    # Name of the Azure container
    container_name = ""
    # Base path in the ADLS container. Keep this empty if you want to upload to the root path of the container
    container_base_path = ""

    temp_path = clean_up_local(temp_base_path)
    git_local_clone_path = os.path.join(
        temp_base_path, temp_path, git_project_name)
    clone_git(git_url, git_branch, git_local_clone_path)
    # The path to be removed from the local directory path while uploading it to ADLS
    path_to_remove = os.path.join(temp_base_path, temp_path, git_project_name)
    # The local directory to upload to ADLS
    azure_upload_src_directory = os.path.join(
        temp_base_path, temp_path, upload_src_directory)
    adls_target_path = os.path.join(container_base_path, azure_upload_src_directory.replace(
        path_to_remove, "").lstrip("\\").lstrip("/"))
    print("ADLS Location to upload the files -->", adls_target_path)
    print("Checking and cleaning up ADLS")
    delete_adls_directory(connect_str, container_name, adls_target_path)
    print("Uploading files to ADLS")
    upload_data_to_adls(connect_str, container_name, path_to_remove,
                        azure_upload_src_directory, container_base_path)


if __name__ == '__main__':
    # Main invoke.
    main()
