import os

from pkrfilesorter.sorters.abstract import AbstractFileSorter


class LocalFileSorter(AbstractFileSorter):

    def __init__(self, source_dir: str, data_dir: str):
        self.source_dir = self.correct_source_dir(source_dir)
        self.data_dir = self.correct_source_dir(data_dir)
        self.sorted_files_record = "copied_files.txt"

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

    def write_source_file_to_raw_file(self, source_key: str, raw_key: str):
        """
        Write a source file to a raw file
        """
        raw_dir = os.path.dirname(raw_key)
        os.makedirs(raw_dir, exist_ok=True)
        os.chmod(raw_dir, 0o777)
        with open(source_key, 'r', encoding='utf-8') as source_file:
            source_content = source_file.read()
            source_content = self.correct_file_content(source_content)
        with open(raw_key, 'w', encoding='utf-8') as raw_file:
            raw_file.write(source_content)
        os.chmod(raw_key, 0o777)
        print(f"File {source_key} written to {raw_key}")
        self.add_to_sorted_files(source_key)
