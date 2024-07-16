import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError
from typing import List, Dict


class S3FileSorter:
    """
    A class to sort files from a source directory to an S3 bucket
    """
    def __init__(self, source_dir: str, destination_bucket: str):
        self.source_dir = source_dir
        self.destination_bucket = destination_bucket
        self.s3 = boto3.client('s3')

    def get_source_files(self) ->list[dict]:
        """
        Get all txt files in the source directory and its subdirectories
        """
        files_dict = [{"root": root, "filename": file}
                      for root, _, files in os.walk(self.source_dir) for file in files if file.endswith(".txt")]
        return files_dict

    def correct_source_files(self):
        """
        Correct the corrupted files in the source directory
        """
        files_dict = self.get_source_files()
        corrupted_files = [file for file in files_dict if file.get("filename").startswith("summary")]
        # Change the filename of the corrupted files
        for file in corrupted_files:
            new_filename = file.get("filename")[7:]
            base_path = os.path.join(file.get("root"), file.get("filename"))
            new_path = os.path.join(file.get("root"), new_filename)
            os.replace(base_path, new_path)
            print(f"File {base_path} renamed to {new_filename}")

    @staticmethod
    def get_date(filename: str) -> str:
        """
        Get the date of the file
        """
        date_str = filename.split("_")[0]
        date_path = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
        return date_path

    def get_destination_key(self, filename: str) -> str:
        """
        Get the destination key of the file
        """
        date_path = self.get_date(filename)
        file_type = "summaries" if "summary" in filename else "histories/raw"
        destination_key = f"data/{file_type}/{date_path}/{filename}"
        return destination_key

    def check_file_exists(self, key: str) -> bool:
        """
        Check if a file exists in the S3 bucket
        """
        try:
            self.s3.head_object(Bucket=self.destination_bucket, Key=key)
            return True
        except ClientError:
            return False

    def upload_files(self):
        """
        Upload files from the source directory to the S3 bucket
        """
        for file in self.get_source_files():
            file_root = file.get("root")
            filename = file.get("filename")
            source_path = os.path.join(file_root, filename)
            destination_key = self.get_destination_key(filename)
            copy_condition = "positioning_file" not in filename and "omaha" not in filename and "play" not in filename
            if not self.check_file_exists(destination_key) and copy_condition:
                try:
                    self.s3.upload_file(source_path, self.destination_bucket, destination_key)
                    print(f"File {source_path} copied to s3://{self.destination_bucket}/{destination_key}")
                except NoCredentialsError:
                    print("Credentials not available")
                except ClientError as e:
                    print(f"An error occurred while uploading {filename}: {e}")
            else:
                print(f"File {destination_key} already exists in the bucket")