import os

from pkrfilesorter.sorters.abstract import AbstractFileSorter


class LocalFileSorter(AbstractFileSorter):

    def __init__(self, source_dir: str, local_data_dir: str):
        self.source_dir = self.correct_source_dir(source_dir)
        self.data_dir = self.local_data_dir = self.correct_source_dir(local_data_dir)
        self.sorted_files_record_path = os.path.join(self.local_data_dir, "copied_files.txt")
        self.name_corrections_file_key = os.path.join(self.data_dir, "name_corrections.json")
        self.names_to_correct_file_key = os.path.join(self.data_dir, "names_to_correct.txt")
        self.parsed_corrections_dir = os.path.join(self.corrections_dir, "histories", "parsed")
        self.correction_raw_keys_file_key = os.path.join(self.data_dir, "correction_raw_keys.txt")

    def check_raw_key_exists(self, raw_key: str) -> bool:
        """
        Check if a file raw key exists in the destination directory
        """
        return os.path.exists(raw_key)

    def get_raw_key(self, raw_key_suffix: str) -> str:
        """
        Get the raw key of a file
        """
        return os.path.join(self.data_dir, raw_key_suffix)

    def write_source_file_to_raw_file(self, source_path: str, raw_key: str):
        """
        Write a source file to a raw file
        """
        raw_dir = os.path.dirname(raw_key)
        os.makedirs(raw_dir, exist_ok=True)
        os.chmod(raw_dir, 0o777)
        source_content = self.get_local_file_content(source_path)
        source_content = self.correct_file_content(source_content)
        self.write_file(raw_key, source_content)
        os.chmod(raw_key, 0o777)
        print(f"File {source_path} written to {raw_key}")
        self.add_to_sorted_files(source_path)

    def list_parsed_correction_keys(self) -> list:
        """
        Lists all the parsed files in to correct

        """
        correction_keys = [os.path.join(root, filename)
                           for root, _, files in os.walk(self.parsed_corrections_dir)
                           for filename in files if filename.endswith(".json")]
        return correction_keys

    def get_file_content(self, key: str) -> str:
        return self.get_local_file_content(key)

    def write_file(self, file_key: str, content: str):
        self.write_local_file(file_key, content)

    def write_file_from_list(self, file_key: str, content: list):
        self.write_local_file_from_list(file_key, content)
