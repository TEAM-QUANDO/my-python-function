from azure.storage.blob import BlobServiceClient
import csv


def create_csv(blob_list):
    try:
        # csv file header row (first row)
        header = ["blob_name"]
        file_name = "blob_list_output.csv"      # csv filename

        with open(file_name, 'w', newline='', encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(header)             # write header
            for blob in blob_list:
                writer.writerow([blob])

        print('Created CSV.')
    except Exception as ex:
        print('create_csv | Error: ', ex)


def azure_connect_conn_string(source_container_connection_string, source_container_name):
    try:
        blob_source_service_client = BlobServiceClient.from_connection_string(
            source_container_connection_string)
        source_container_client = blob_source_service_client.get_container_client(
            source_container_name)
        print("Connection String -- Connected.")
        return source_container_client

    except Exception as ex:
        print("Error: " + str(ex))


def azure_connect_sas_token(token, account_url, source_container_name):
    try:
        blob_source_service_client = BlobServiceClient(
            account_url=account_url, credential=token)
        source_container_client = blob_source_service_client.get_container_client(
            source_container_name)
        print("SAS Token -- Connected.")
        return source_container_client

    except Exception as ex:
        print("Error: " + str(ex))


def azure_connect_sas_url(source_container_sas_url, source_container_name):
    try:
        blob_source_service_client = BlobServiceClient(
            source_container_sas_url)
        source_container_client = blob_source_service_client.get_container_client(
            source_container_name)
        print("SAS URL -- Connected.")
        return source_container_client

    except Exception as ex:
        print("Error: " + str(ex))


def container_content_list(connection_instance, blob_path):
    try:
        blob_name_list = []
        source_blob_list = connection_instance.list_blobs(
            name_starts_with=blob_path)
        for blob in source_blob_list:
            blob_name = blob.name.rsplit('/', 1)[1]
            blob_name_list.append(blob_name)
            print(blob_name)

        create_csv(blob_name_list)

    except Exception as ex:
        print("Error: " + str(ex))


def main():
    try:
        blob_path = ''
        connection_instance = None

        print('\nConnection method menu:')
        print('1. Connection string')
        print('2. SAS token')
        print('3. SAS URL')

        flag = int(input('\nPlease enter connection method (1,2 or 3): '))

        if flag == 1:
            print('\nConnection string selected')

            # Connection String
            azure_connection_string = input(
                'Please enter Container connection string: ')
            container_name = input('Please enter Container name: ')
            blob_path = input('Please enter blob path (should end with a /): ')
            connection_instance = azure_connect_conn_string(
                azure_connection_string, container_name)

        elif flag == 2:
            print('\nSAS Token selected')
            # SAS Token
            azure_sas_token = input('Please enter SAS Token: ')
            azure_acc_url = input('Please enter Account URL: ')
            container_name = input('Please enter Container name: ')
            blob_path = input('Please enter blob path (should end with a /): ')
            connection_instance = azure_connect_sas_token(
                azure_sas_token, azure_acc_url, container_name)

        elif flag == 3:
            print('\nSAS URL selected')
            # SAS URL
            azure_sas_url = input('Please enter SAS URL: ')
            container_name = input('Please enter Container name: ')
            blob_path = input('Please enter blob path (should end with a /): ')
            connection_instance = azure_connect_sas_url(
                azure_sas_url, container_name)

        else:
            print('Wrong option!!')
            exit()

        # List Blobs
        container_content_list(connection_instance, blob_path)

        print('Done')

    except Exception as ex:
        print('main | Error: ', ex)


if __name__ == "__main__":
    main()
