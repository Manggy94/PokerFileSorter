import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from pkrfilesorter.sorters.abstract import AbstractFileSorter


class CloudFileSorter(AbstractFileSorter):

    def __init__(self, source_dir: str, data_dir: str, bucket_name: str):
        self.source_dir = source_dir
        self.data_dir = data_dir
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3')
        self.sorted_files_record = "uploaded_files.txt"
        self.data_prefix = "data"

    def check_raw_key_exists(self, raw_key: str) -> bool:
        """
        Check if a file raw key exists in the destination directory
        """
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=raw_key)
        raw_key_exists = bool(response.get("Contents"))
        return raw_key_exists

    def get_raw_key(self, raw_key_suffix: str) -> str:
        """
        Get the raw key of a file
        """
        return f"{self.data_prefix}/{raw_key_suffix}"

    def write_source_file_to_raw_file(self, source_key: str, raw_key: str):
        """
        Write a source file to a raw file
        """
        try:
            with open(source_key, 'r', encoding='utf-8') as source_file:
                source_content = source_file.read()
            source_content = self.correct_file_content(source_content)
            self.s3.put_object(Bucket=self.bucket_name, Key=raw_key, Body=source_content)
            #self.s3.upload_file(source_key, self.bucket_name, raw_key)
            print(f"File {source_key} written to s3://{raw_key}")
            self.add_to_sorted_files(source_key)
        except NoCredentialsError:
            print("Credentials not available")
        except ClientError as e:
            print(f"An error occurred while uploading {source_key}: {e}")

