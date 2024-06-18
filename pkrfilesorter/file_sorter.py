"""This module contains the FileSorter class which is responsible for copying files from a source directory to a
specific destination directory."""
import os


class FileSorter:
    """
    A class to sort files from a source directory to a destination directory

    Attributes:
        source_dir (str): The source directory
        destination_dir (str): The destination directory

    Methods:
    """
    def __init__(self, source_dir: str, destination_dir: str):
        self.source_dir = source_dir
        self.destination_dir = destination_dir

    def get_source_files(self) -> list[dict]:
        """
        Get all txt files in the source directory and its subdirectories

        Returns:
            files_dict (list[dict]): A list of dictionaries containing the root directory and filename of the files
        """
        files_dict = [{"root": root, "filename": file} for root, _, files in os.walk(self.source_dir)
                for file in files if file.endswith(".txt")]
        return files_dict

    @staticmethod
    def get_date(filename: str) -> str:
        """
        Get the date of the file

        Args:
            filename (str): The filename of the file

        Returns:
            date_path (str): The date path of the file
        """
        date_str = filename.split("_")[0]
        date_path = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
        return date_path

    def get_destination_path(self, filename: str) -> str:
        """
        Get the destination path of the file

        Args:
            filename (str): The filename of the file

        Returns:
            destination_path (str): The destination path of the file
        """
        date_path = self.get_date(filename)
        file_type = "summaries" if "summary" in filename else "histories/raw"
        destination_path = os.path.join(self.destination_dir, file_type, date_path, filename)
        return destination_path

    def get_source_path(self, filename: str) -> str:
        """
        Get the absolute source directory path of the file

        Args:
            filename (str): The filename of the file

        Returns:
            source_path (str): The source path of the file
        """
        source_path = os.path.join(self.source_dir, filename)
        return source_path

    def check_file_exists(self, filename: str) -> bool:
        """
        Check if the file already exists in the destination directory

        Args:
            filename (str): The filename to check

        Returns:
            (bool): True if the file already exists, False otherwise
        """
        return os.path.exists(self.get_destination_path(filename))

    def copy_files(self):
        """
        Copy all files from the source directory to the destination directory
        """
        for file in self.get_source_files():
            file_root = file.get("root")
            filename = file.get("filename")
            source_path = os.path.join(file_root, filename)
            destination_path = self.get_destination_path(filename)
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            if not self.check_file_exists(filename):
                with open(source_path, "r", encoding="utf-8") as source_file:
                    with open(destination_path, "w", encoding="utf-8") as destination_file:
                        destination_file.write(source_file.read())
                print(f"File {filename} copied to {destination_path}")

