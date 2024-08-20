import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError

from pkrfilesorter.sorters.abstract import AbstractFileSorter


class CloudFileSorter(AbstractFileSorter):

    def __init__(self, source_dir: str, local_data_dir: str, bucket_name: str):
        self.source_dir = source_dir
        self.local_data_dir = local_data_dir
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3')
        self.sorted_files_record_path = os.path.join(self.local_data_dir, "uploaded_files.txt")
        self.data_dir = "data"
        self.name_corrections_file_key = "data/name_corrections.json"
        self.parsed_corrections_dir = "data/corrections/parsed"
        self.correction_raw_keys_file_key = "data/correction_raw_keys.txt"

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
        return f"{self.data_dir}/{raw_key_suffix}"

    def write_source_file_to_raw_file(self, source_path: str, raw_key: str):
        """
        Write a source file to a raw file
        """
        try:
            source_content = self.get_local_file_content(source_path)
            source_content = self.correct_file_content(source_content)
            self.write_file(raw_key, source_content)
            print(f"File {source_path} written to s3://{raw_key}")
            self.add_to_sorted_files(source_path)
        except ClientError as e:
            print(f"An error occurred while uploading {source_path}: {e}")

    def list_parsed_correction_keys(self) -> list:
        """
        Lists all the parsed files in to correct

        """
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.parsed_corrections_dir)
        keys = [obj["Key"] for page in pages for obj in page.get("Contents", [])]
        return keys

    def get_file_content(self, key: str) -> str:
        response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
        content = response["Body"].read().decode("utf-8")
        return content

    def write_file(self, file_key: str, content: str):
        self.s3.put_object(Bucket=self.bucket_name, Key=file_key, Body=content)

    def write_file_from_list(self, file_key: str, content: list):
        content = "\n".join(content)
        self.s3.put_object(Bucket=self.bucket_name, Key=file_key, Body=content)
