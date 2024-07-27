"""This module contains the FileSorter class which is responsible for copying files from a source directory to a
specific raw directory."""
import os
import re
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed


class AbstractFileSorter(ABC):
    """
    """
    source_dir: str
    data_dir: str
    sorted_files_record: str

    @staticmethod
    def correct_source_dir(source_dir: str) -> str:
        """
        Correct the source directory path
        """
        if not os.path.exists(source_dir):
            source_dir = source_dir.replace("C:/", "/mnt/c/")
        return source_dir

    def list_source_files_dict(self) -> list[dict]:
        """
        Get all txt files in the source directory and its subdirectories

        Returns:
            files_dict (list[dict]): A list of dictionaries containing the root directory and filename of the files
        """
        files_dict = [{"root": root, "filename": file}
                      for root, _, files in os.walk(self.source_dir) for file in files if file.endswith(".txt")]
        return files_dict

    def list_source_history_keys(self) -> list:
        """
        Lists all the history files in the source directory and returns a list of their root, and file names

        Returns:
            list: A list of dictionaries containing the root and filename of the history files
        """
        source_keys = [os.path.join(root, filename)
                       for root, _, files in os.walk(self.source_dir)
                       for filename in files if filename.endswith(".txt")]
        return source_keys

    def correct_source_files(self):
        """
        Correct the corrupted files in the source directory
        """
        files_dict = self.list_source_files_dict()
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
        info_dict["source_key"] = file_path
        info_dict["file_name"] = file_name
        return info_dict

    @staticmethod
    def get_info_from_filename(filename: str) -> dict:
        """
        Get the date and raw key of a file
        """
        tournament_pattern = re.compile(r"\((\d+)\)_")
        cash_game_pattern = re.compile(r"_([\w\s]+)(\d{2})_")
        cash_game_pattern2 = re.compile(r"(\d{4})(\d{2})(\d{2})_([A-Za-z]+)")
        date_str = filename.split("_")[0]
        date_path = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
        file_type = "summaries/raw" if "summary" in filename else "histories/raw"
        match1 = tournament_pattern.search(filename)
        match2 = cash_game_pattern.search(filename)
        match3 = cash_game_pattern2.search(filename)
        is_play = "play" in filename
        is_omaha = "omaha" in filename or "Omaha" in filename
        is_positioning = "positioning" in filename
        if match1:
            tournament_id = match1.group(1)
            raw_key_suffix = f"{file_type}/{date_path}/{tournament_id}.txt"
            is_tournament = True
        elif match2:
            table_name = match2.group(1).strip().replace(" ", "_")
            table_id = match2.group(2)
            raw_key_suffix = f"{file_type}/{date_path}/cash/{table_name}/{table_id}.txt"
            is_tournament = False
        elif match3:

            table_name = match3.group(4).strip()
            table_id = "000000000"
            raw_key_suffix = f"{file_type}/{date_path}/cash/{table_name}/{table_id}.txt"
            is_tournament = False
        else:
            raw_key_suffix = is_tournament = None
        file_info = {
            "date": date_path,
            "raw_key_suffix": raw_key_suffix,
            "is_tournament": is_tournament,
            "is_play": is_play,
            "is_omaha": is_omaha,
            "is_positioning": is_positioning,
            "is_error": any((is_play, is_omaha, is_positioning, not is_tournament))
        }
        return file_info

    @abstractmethod
    def get_raw_key(self, raw_key_suffix: str) -> str:
        """
        Get the raw key of a file
        """
        pass

    def get_error_files(self) -> list:
        """
        Get the files listed in the error_files.txt file
        """
        file_location = os.path.join(self.data_dir, "error_files.txt")
        with open(file_location, "r") as file:
            error_files = file.read().splitlines()
        return error_files

    def add_to_error_files(self, source_key: str):
        """
        Add a filename to the error_files.txt file
        """
        file_location = os.path.join(self.data_dir, "error_files.txt")
        with open(file_location, "a") as file:
            file.write(f"{source_key}\n")
        print(f"File {source_key} added to {file_location}")

    @abstractmethod
    def check_raw_key_exists(self, raw_key: str) -> bool:
        """
        Check if a file raw key exists in the raw directory
        """
        pass
    
    def get_sorted_files(self):
        """
        Get the files listed in the <sorted_files>.txt file
        """
        file_location = os.path.join(self.data_dir, self.sorted_files_record)
        with open(file_location, "r") as file:
            sorted_files = file.read().splitlines()
        return sorted_files
    
    def add_to_sorted_files(self, source_key: str):
        """
        Add a filename to the sorted_files.txt file
        """
        file_location = os.path.join(self.data_dir, self.sorted_files_record)
        with open(file_location, "a") as file:
            file.write(f"{source_key}\n")
        print(f"File {source_key} added to {file_location}")

    def reset_sorted_files(self):
        """
        Reset the sorted_files.txt file
        """
        file_location = os.path.join(self.data_dir, self.sorted_files_record)
        with open(file_location, "w") as file:
            file.write("")
        print(f"{file_location} reset successfully")

    @abstractmethod
    def write_source_file_to_raw_file(self, source_key: str, raw_key: str):
        """
        Write a source file to a raw file
        """
        pass

    def sort_file(self, file_dict: dict):
        """
        Sort a file from the source directory to the raw directory
        """
        file_info = self.get_file_info(file_dict)
        source_key = file_info.get("source_key")
        raw_key_suffix = file_info.get("raw_key_suffix")
        raw_key = self.get_raw_key(raw_key_suffix)
        is_error_file = file_info.get("is_error")
        if source_key not in self.get_error_files() and not is_error_file:
            self.write_source_file_to_raw_file(source_key, raw_key)
        elif source_key not in self.get_error_files() and is_error_file:
            self.add_to_error_files(source_key)

    def sort_new_file(self, file_dict: dict):
        """
        Upload a file to the S3 bucket
        """
        file_info = self.get_file_info(file_dict)
        source_key = file_info.get("source_key")
        raw_key = self.get_raw_key(file_info.get("raw_key_suffix"))
        if source_key not in self.get_sorted_files() and not self.check_raw_key_exists(raw_key):
            self.sort_file(file_dict)
        elif source_key not in self.get_sorted_files() and self.check_raw_key_exists(raw_key):
            self.add_to_sorted_files(source_key)

    def sort_files(self):
        """
        Upload files from the source directory to the S3 bucket
        """
        self.correct_source_files()
        self.reset_sorted_files()
        files_to_sort = self.list_source_files_dict()[::-1]
        with ThreadPoolExecutor() as executor:
            future_to_file = {executor.submit(self.sort_file, file): file for file in files_to_sort}
            for future in as_completed(future_to_file):
                future.result()
        print("All files sorted successfully")

    def sort_new_files(self):
        self.correct_source_files()
        files_to_sort = self.list_source_files_dict()[::-1]
        with ThreadPoolExecutor() as executor:
            future_to_file = {executor.submit(self.sort_new_file, file): file for file in files_to_sort}
            for future in as_completed(future_to_file):
                future.result()
        print("All new files sorted successfully")
