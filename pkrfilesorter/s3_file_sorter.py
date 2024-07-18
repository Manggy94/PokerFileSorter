import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed


class S3FileSorter:
    """
    A class to sort files from a source directory to an S3 bucket
    """
    def __init__(self, source_dir: str, destination_bucket: str, data_dir: str):
        self.data_dir = data_dir
        self.source_dir = source_dir
        self.destination_bucket = destination_bucket
        self.s3 = boto3.client('s3')

    def get_source_files(self) -> list[dict]:
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

    def get_uploaded_files(self) -> list:
        """
        Get the files listed in the uploaded_files.txt file
        """
        file_location = os.path.join(self.data_dir, "uploaded_files.txt")
        with open(file_location, "r") as file:
            uploaded_files = file.read().splitlines()
        return uploaded_files

    def get_error_files(self) -> list:
        """
        Get the files listed in the error_files.txt file
        """
        file_location = os.path.join(self.data_dir, "error_files.txt")
        with open(file_location, "r") as file:
            error_files = file.read().splitlines()
        return error_files

    def add_to_uploaded_files(self, filename: str):
        """
        Add a filename to the uploaded_files.txt file
        """
        file_location = os.path.join(self.data_dir, "uploaded_files.txt")
        with open(file_location, "a") as file:
            file.write(f"{filename}\n")
        print(f"File {filename} added to {file_location}")

    def add_to_error_files(self, filename: str):
        """
        Add a filename to the error_files.txt file
        """
        file_location = os.path.join(self.data_dir, "error_files.txt")
        with open(file_location, "a") as file:
            file.write(f"{filename}\n")
        print(f"File {filename} added to {file_location}")

    def get_src_and_dest(self, file: dict):
        file_root = file.get("root")
        filename = file.get("filename")
        source_path = os.path.join(file_root, filename)
        destination_key = self.get_destination_key(filename)
        return source_path, destination_key

    def upload_file(self, file: dict, check_exists: bool = False):
        uploaded_files = self.get_uploaded_files() if check_exists else []
        error_files = self.get_error_files()
        source_path, destination_key = self.get_src_and_dest(file)
        filename = file.get("filename")
        error_condition = "positioning_file" in filename or "omaha" in filename or "play" in filename
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
                    self.s3.upload_new_file(source_path, self.destination_bucket, destination_key)
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
        files_to_upload = self.get_source_files()[:100]
        with ThreadPoolExecutor(max_workers=1) as executor:
            future_to_file = {executor.submit(self.reupload_file, file): file for file in files_to_upload}
            for future in as_completed(future_to_file):
                future.result()
        print("All files uploaded successfully")

    def upload_new_files(self):
        self.correct_source_files()
        files_to_upload = self.get_source_files()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future_to_file = {executor.submit(self.upload_new_file, file): file for file in files_to_upload}
            for future in as_completed(future_to_file):
                future.result()
        print("All files uploaded successfully")
