from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

# Tenant ID for your Azure Subscription
TENANT_ID = "85XXX93e-XXXX-XXXX-XXXXX-96150XXX893e"

# Your Service Principal App ID (Client ID)
CLIENT_ID = "a3XXX40d-xxxxxxx-0ff72XXXX66a"

# Your Service Principal Password (Client Secret)
CLIENT_SECRET = "5XXXIdPxGEXXXX_1H8XXy0kao_7"

ACCOUNT_NAME = "azXXXxxxXX"

CONTAINER_NAME = "XXXXXXXXXX"

credentials = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)

blobService = BlobServiceClient(
    "https://{}.blob.core.windows.net".format(ACCOUNT_NAME),
    credential=credentials
)

print("\n==============LIST OF ALL BLOBS=================")
# Path in the container. If you want to list everything in the root path, keep it empty
prefix = ""

container = blobService.get_container_client(CONTAINER_NAME)

for blob in container.list_blobs(name_starts_with=prefix):
    print("\t Blob name: " + blob.name)
