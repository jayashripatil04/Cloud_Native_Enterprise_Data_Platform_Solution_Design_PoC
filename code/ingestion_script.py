import logging
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime
import shutil


def upload_files_from_local_to_s3(parent_directory, bucket):
    s3_client = boto3.client('s3')

    print(f"Directory passed: {parent_directory}")

    for dataset in os.listdir(parent_directory):
        directory = os.path.join(parent_directory, dataset)

        if not os.path.isdir(directory):
            continue

        #dataset = os.path.basename(directory)
        print(f"\nProcessing dataset: {dataset}")
        prefix = f"raw/{dataset}"

        archive_dir = os.path.join(directory, "archive")
        os.makedirs(archive_dir, exist_ok=True)

        files = os.listdir(directory)

        csv_files = [f for f in files if f.lower().endswith(".csv")]

        if not csv_files:
            print("No files found in the directory.")
            continue
    
        for file in csv_files:          
            file_path = os.path.join(directory, file)

            base_name = os.path.splitext(file)[0]
            extension = os.path.splitext(file)[1]

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            new_file_name = f"{base_name}_{timestamp}{extension}"

            today = datetime.now().strftime("%Y/%m/%d")
            object_name = f"{prefix}/{today}/{new_file_name}"
            
            try:
                s3_client.upload_file(file_path, bucket, object_name)
                print(f"Uploaded: {file} → {object_name}")
            except ClientError as e:
                logging.error(f"Error uploading {file}: {e}")

            archive_path = os.path.join(archive_dir, file)
            shutil.move(file_path, archive_path)

            print(f"files moved to archive")

        logging.basicConfig(
        filename='ingestion.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
        )

        try:
            s3_client.upload_file('ingestion.log', 'etl-data-platform-poc', 'logs/ingestion/ingestion.log')
            logging.info(f"Uploaded {file} to {object_name}")
        except ClientError as e:
            logging.error(f"Error uploading {file}: {e}")


upload_files_from_local_to_s3('C:\\Users\\Jayashri\\Desktop\\Code\\Data_Files', 'etl-data-platform-poc')