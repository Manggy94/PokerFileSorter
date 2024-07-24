import boto3
import os
import re
from botocore.exceptions import NoCredentialsError, ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

from file_sorter import FileSorter


class S3FileSorter(FileSorter):
    """
    A class to sort files from a source directory to an S3 bucket
    """
    def __init__(self, source_dir: str, destination_bucket: str, data_dir: str):
        self.data_dir = data_dir
        self.source_dir = source_dir
        self.destination_bucket = destination_bucket
        self.s3 = boto3.client('s3')

    def check_file_exists(self, key: str) -> bool:
        """
        Check if a file exists in the S3 bucket
        """
        try:
            self.s3.head_object(Bucket=self.destination_bucket, Key=key)
            return True
        except ClientError:
            return False

    def get_uploaded_files(self) -> list:
        """
        Get the files listed in the uploaded_files.txt file
        """
        file_location = os.path.join(self.data_dir, "uploaded_files.txt")
        with open(file_location, "r") as file:
            uploaded_files = file.read().splitlines()
        return uploaded_files

    def add_to_uploaded_files(self, filename: str):
        """
        Add a filename to the uploaded_files.txt file
        """
        file_location = os.path.join(self.data_dir, "uploaded_files.txt")
        with open(file_location, "a") as file:
            file.write(f"{filename}\n")
        print(f"File {filename} added to {file_location}")

    def upload_file(self, file: dict, check_exists: bool = False):
        file_info = self.get_file_info(file)
        uploaded_files = self.get_uploaded_files() if check_exists else []
        error_files = self.get_error_files()
        is_positioning = file_info.get("is_positioning")
        is_play = file_info.get("is_play")
        is_omaha = file_info.get("is_omaha")
        other_than_tournament = not file_info.get("is_tournament")
        source_path = file_info.get("file_path")
        destination_key = file_info.get("destination_key")
        filename = file_info.get("file_name")
        error_condition = any((is_play, is_omaha, is_positioning, other_than_tournament))
        copy_condition = filename not in uploaded_files + error_files
        if copy_condition:
            print(f"Copying file {filename} to s3://{self.destination_bucket}/{destination_key}")
            if error_condition:
                self.add_to_error_files(filename)
                print("Error: File is not allowed to be copied because it is an error file")
            elif self.check_file_exists(destination_key) and check_exists:
                self.add_to_uploaded_files(filename)
                print(f"Error: File {filename} already exists in the bucket")
            else:
                try:
                    self.s3.upload_file(source_path, self.destination_bucket, destination_key)
                    print(f"File {source_path} copied to s3://{self.destination_bucket}/{destination_key}")
                    self.add_to_uploaded_files(filename)
                except NoCredentialsError:
                    print("Credentials not available")
                except ClientError as e:
                    print(f"An error occurred while uploading {filename}: {e}")

    def upload_new_file(self, file: dict):
        """
        Upload a file to the S3 bucket
        """
        self.upload_file(file, check_exists=True)

    def reupload_file(self, file: dict):
        """
        Re-upload a file to the S3 bucket
        """
        self.upload_file(file, check_exists=False)

    def upload_files(self):
        """
        Upload files from the source directory to the S3 bucket
        """
        self.correct_source_files()
        files_to_upload = self.get_source_files()[::-1]
        with ThreadPoolExecutor(max_workers=6) as executor:
            future_to_file = {executor.submit(self.reupload_file, file): file for file in files_to_upload}
            for future in as_completed(future_to_file):
                future.result()
        print("All files uploaded successfully")

    def upload_new_files(self):
        self.correct_source_files()
        files_to_upload = self.get_source_files()[::-1]
        with ThreadPoolExecutor(max_workers=1) as executor:
            future_to_file = {executor.submit(self.upload_new_file, file): file for file in files_to_upload}
            for future in as_completed(future_to_file):
                future.result()
        print("All files uploaded successfully")
