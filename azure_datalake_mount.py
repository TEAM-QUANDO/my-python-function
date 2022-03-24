######################################################################################
# Set the configurations. Here's what you need:
# 1.) Client ID (a.k.a Application ID)
# 2.) Client Secret (a.k.a. Application Secret)
# 3.) Directory ID
# 4.) File System Name
# 5.) Storage Account Name
# 6.) Mount Name
######################################################################################
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "<client-id>",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>", key="<key-name-for-service-credential>"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

######################################################################################
# Optionally, you can add <directory-name> to the source URI of your mount point.
######################################################################################
dbutils.fs.mount(
    source="abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/",
    mount_point="/mnt/<mount-name>",
    extra_configs=configs)
