"""This module contains the FileSorter class which is responsible for copying files from a source directory to a
specific destination directory."""
import os
import re


class FileSorter:
    """
    A class to sort files from a source directory to a destination directory

    Attributes:
        data_dir (str): The data directory
        source_dir (str): The source directory

    Methods:
        get_source_files: Get all txt files in the source directory and its subdirectories
        get_date: Get the date of the file
        get_destination_path: Get the destination path of the file
        get_source_path: Get the absolute source directory path of the file
        check_file_exists: Check if the file already exists in the destination directory
        copy_files: Copy all files from the source directory to the destination directory

    Examples:
        file_sorter = FileSorter("source_dir", "destination_dir")
        file_sorter.copy_files()
    """
    source_dir: str
    data_dir: str

    def get_source_files(self) -> list[dict]:
        """
        Get all txt files in the source directory and its subdirectories

        Returns:
            files_dict (list[dict]): A list of dictionaries containing the root directory and filename of the files
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
        cash_game_pattern2 = re.compile(r"(\d{4})(\d{2})(\d{2})_([A-Za-z]+)")
        date_str = filename.split("_")[0]
        date_path = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
        file_type = "summaries" if "summary" in filename else "histories/raw"
        match1 = tournament_pattern.search(filename)
        match2 = cash_game_pattern.search(filename)
        match3 = cash_game_pattern2.search(filename)
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
        elif match3:

            table_name = match3.group(4).strip()
            table_id = "000000000"
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

    def get_error_files(self) -> list:
        """
        Get the files listed in the error_files.txt file
        """
        file_location = os.path.join(self.data_dir, "error_files.txt")
        with open(file_location, "r") as file:
            error_files = file.read().splitlines()
        return error_files

    def add_to_error_files(self, filename: str):
        """
        Add a filename to the error_files.txt file
        """
        file_location = os.path.join(self.data_dir, "error_files.txt")
        with open(file_location, "a") as file:
            file.write(f"{filename}\n")
        print(f"File {filename} added to {file_location}")
