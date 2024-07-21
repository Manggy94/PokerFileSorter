import boto3
import os
import re
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

    def get_file_info(self, file_dict: dict):
        file_name = file_dict.get("filename")
        file_root = file_dict.get("root")
        file_path = os.path.join(file_root, file_name)
        info_dict = self.get_info_from_filename(file_name)
        info_dict["file_path"] = file_path
        info_dict["file_name"] = file_name
        return info_dict

    @staticmethod
    def get_info_from_filename(filename: str) -> dict:
        """
        Get the date and destination key of a file
        """
        tournament_pattern = re.compile(r"\((\d+)\)_")
        cash_game_pattern = re.compile(r"_([\w\s]+)(\d{2})_")
        date_str = filename.split("_")[0]
        date_path = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
        file_type = "summaries" if "summary" in filename else "histories/raw"
        match1 = tournament_pattern.search(filename)
        match2 = cash_game_pattern.search(filename)
        is_play = "play" in filename
        is_omaha = "omaha" in filename
        is_positioning = "positioning" in filename
        if match1:
            tournament_id = match1.group(1)
            destination_key = f"data/{file_type}/{date_path}/{tournament_id}.txt"
            is_tournament = True
        elif match2:
            table_name = match2.group(1).strip().replace(" ", "_")
            table_id = match2.group(2)
            destination_key = f"data/{file_type}/{date_path}/cash/{table_name}/{table_id}.txt"
            is_tournament = False
        else:
            destination_key = is_tournament = None
        file_info = {
            "date": date_path,
            "destination_key": destination_key,
            "is_tournament": is_tournament,
            "is_play": is_play,
            "is_omaha": is_omaha,
            "is_positioning": is_positioning,
        }
        return file_info

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
        files_to_upload = self.get_source_files()[::-1][:10]
        with ThreadPoolExecutor(max_workers=6) as executor:
            future_to_file = {executor.submit(self.reupload_file, file): file for file in files_to_upload}
            for future in as_completed(future_to_file):
                future.result()
        print("All files uploaded successfully")

    def upload_new_files(self):
        self.correct_source_files()
        files_to_upload = self.get_source_files()[::-1][:10]
        with ThreadPoolExecutor(max_workers=1) as executor:
            future_to_file = {executor.submit(self.upload_new_file, file): file for file in files_to_upload}
            for future in as_completed(future_to_file):
                future.result()
        print("All files uploaded successfully")
