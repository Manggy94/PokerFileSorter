import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from file_sorter import FileSorter


class LocalFileSorter(FileSorter):
    def __init__(self, source_dir: str, data_dir: str):
        self.data_dir = data_dir
        self.source_dir = source_dir

    def get_destination_path(self, destination_key: str) -> str:
        """
        Get the destination path of a file
        """
        return os.path.join(self.data_dir, destination_key).replace("data/data", "data").replace("data\data", "data")

    def check_file_exists(self, destination_key: str) -> bool:
        """
        Check if a file exists in the destination dir
        """
        destination_path = self.get_destination_path(destination_key)
        return os.path.exists(destination_path)

    def get_copied_files(self) -> list:
        """
        Get the files listed in the copied_files.txt file
        """
        file_location = os.path.join(self.data_dir, "copied_files.txt")
        with open(file_location, "r") as file:
            copied_files = file.read().splitlines()
        return copied_files

    def add_to_copied_files(self, filename: str):
        """
        Add a filename to the copied_files.txt file
        """
        file_location = os.path.join(self.data_dir, "copied_files.txt")
        with open(file_location, "a") as file:
            file.write(f"{filename}\n")
        print(f"File {filename} added to {file_location}")

    def copy_file(self, file: dict, check_exists: bool = False):
        try:
            file_info = self.get_file_info(file)
            copied_files = self.get_copied_files() if check_exists else []
            error_files = self.get_error_files()
            is_positioning = file_info.get("is_positioning")
            is_play = file_info.get("is_play")
            is_omaha = file_info.get("is_omaha")
            other_than_tournament = not file_info.get("is_tournament")
            source_path = file_info.get("file_path")
            destination_key = file_info.get("destination_key")
            filename = file_info.get("file_name")
            error_condition = any((is_play, is_omaha, is_positioning, other_than_tournament))
            copy_condition = filename not in copied_files + error_files
            if copy_condition:

                if error_condition:
                    self.add_to_error_files(filename)
                    print("Error: File is not allowed to be copied because it is an error file")
                elif self.check_file_exists(destination_key) and check_exists:
                    self.add_to_copied_files(filename)
                    print(f"Error: File {filename} already exists in the bucket")
                else:
                    destination_path = self.get_destination_path(destination_key)
                    print(f"Copying file {filename} to {destination_path}")
                    os.makedirs(os.path.dirname(destination_path), exist_ok=True)
                    with open(source_path, "r", encoding="utf-8") as source_file:
                        with open(destination_path, "w", encoding="utf-8") as destination_file:
                            destination_file.write(source_file.read())
                    self.add_to_copied_files(filename)
                    print(f"File {filename} copied to {destination_path}")
        except TypeError:
            print(f"Error: File {file} could not be copied")
            print(self.get_info_from_filename(file.get("filename")))
            raise TypeError

    def copy_new_file(self, file: dict):
        """
        Upload a file to the S3 bucket
        """
        self.copy_file(file, check_exists=True)

    def recopy_file(self, file: dict):
        """
        Re-copy a file to the S3 bucket
        """
        self.copy_file(file, check_exists=False)

    def copy_files(self):
        """
        Upload files from the source directory to the S3 bucket
        """
        self.correct_source_files()
        files_to_copy = self.get_source_files()[::-1]
        with ThreadPoolExecutor(max_workers=6) as executor:
            future_to_file = {executor.submit(self.recopy_file, file): file for file in files_to_copy}
            for future in as_completed(future_to_file):
                future.result()
        print("All files copied successfully")

    def copy_new_files(self):
        self.correct_source_files()
        files_to_copy = self.get_source_files()[::-1]
        with ThreadPoolExecutor(max_workers=1) as executor:
            future_to_file = {executor.submit(self.copy_new_file, file): file for file in files_to_copy}
            for future in as_completed(future_to_file):
                future.result()
        print("All files copied successfully")
