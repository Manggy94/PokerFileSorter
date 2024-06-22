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
    def __init__(self, source_dir: str, destination_dir: str):
        self.source_dir = source_dir
        self.destination_dir = destination_dir

    def get_source_files(self) -> list[dict]:
        """
        Get all txt files in the source directory and its subdirectories

        Returns:
            files_dict (list[dict]): A list of dictionaries containing the root directory and filename of the files
        """
        files_dict = [{"root": root, "filename": file}
                      for root, _, files in os.walk(self.source_dir) for file in files if file.endswith(".txt")]
        return files_dict

    def correct_source_files(self) -> list[dict]:
        files_dict = self.get_source_files()
        corrupted_files = [file for file in files_dict if file.get("filename").startswith("summary")]
        # Change the filename of the corrupted files
        for file in corrupted_files:
            new_filename = file.get("filename")[7:]
            base_path = os.path.join(file.get("root"), file.get("filename"))
            new_path = os.path.join(file.get("root"), new_filename)
            os.rename(base_path, new_path)
            print(f"File {base_path} renamed to {new_filename}")

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
            if "positioning_file" not in filename:
                os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            if not (self.check_file_exists(filename) or "positioning_file" in filename):
                with open(source_path, "r", encoding="utf-8") as source_file:
                    with open(destination_path, "w", encoding="utf-8") as destination_file:
                        destination_file.write(source_file.read())
                print(f"File {filename} copied to {destination_path}")


if __name__ == "__main__":
    from pkrfilesorter.config import SOURCE_DIR, DESTINATION_DIR
    sorter = FileSorter(SOURCE_DIR, DESTINATION_DIR)
    sorter.correct_source_files()