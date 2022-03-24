from os import getenv
from azure.storage.blob import BlobServiceClient
from io import BytesIO

blob_service_client = BlobServiceClient.from_connection_string(
    getenv("AZURE_STORAGE_CONNECTION_STRING"))


def upload_blob(filename: str, container: str, data: BytesIO):
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container, blob=filename)

        blob_client.upload_blob(data)

        print("success")
    except Exception as e:
        print(e.message)


def download_blob(filename: str, container: str):
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container, blob=filename)

        print(blob_client.download_blob().readall())
    except Exception as e:
        print(e.message)


def delete_blob(filename: str, container: str):
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container, blob=filename)

        blob_client.delete_blob()

        print("success")
    except Exception as e:
        print(e.message)

# Methods for Containers (Folders)


def create_container(container: str):
    try:
        blob_service_client.create_container(container)
        print("success")
    except Exception as e:
        print(e.message)


def delete_container(container: str):
    try:
        blob_service_client.delete_container(container)
        print("success")
    except Exception as e:
        print(e.message)


def get_containers():
    try:
        containers = blob_service_client.list_containers()
        print([container.name for container in containers])
    except Exception as e:
        print(e.message)
