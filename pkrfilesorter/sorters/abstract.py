"""This module contains the FileSorter class which is responsible for copying files from a source directory to a
specific raw directory."""
import json
import os
import re
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed


class AbstractFileSorter(ABC):
    """
    """
    source_dir: str
    data_dir: str
    local_data_dir: str
    parsed_corrections_dir: str
    sorted_files_record_path: str
    name_corrections_file_key: str
    names_to_correct_file_key: str
    correction_raw_keys_file_key: str

    tournament_pattern = re.compile(r"\((\d+)\)_")

    @property
    def corrections_dir(self):
        return self.data_dir.replace("data", "corrections")

    @property
    def error_files_path(self):
        return os.path.join(self.local_data_dir, "error_files.txt")

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

    def list_source_keys(self) -> list:
        """
        Lists all the history files in the source directory and returns a list of their root, and file names

        Returns:
            list: A list of dictionaries containing the root and filename of the history files
        """
        source_keys = [os.path.join(root, filename)
                       for root, _, files in os.walk(self.source_dir)
                       for filename in files if filename.endswith(".txt")]
        return source_keys

    def list_source_tournament_history_keys(self) -> list:
        """
        Lists all the history files in the source directory and returns a list of their root, and file names

        Returns:
            list: A list of dictionaries containing the root and filename of the history files
        """
        source_keys = [os.path.join(root, filename)
                       for root, _, files in os.walk(self.source_dir)
                       for filename in files
                       if "summary" not in filename
                       and filename.endswith(".txt")
                       and re.search(self.tournament_pattern, filename)
                       ]
        return source_keys

    @property
    def tournaments_dict(self) -> dict:
        tournaments_dict = {re.search(self.tournament_pattern, tournament).group(1): tournament
                            for tournament in self.list_source_tournament_history_keys()}
        return tournaments_dict

    @abstractmethod
    def list_parsed_correction_keys(self) -> list:
        """
        Lists all the parsed files in to correct

        """
        pass

    def list_correction_original_files(self) -> list:
        files_to_correct = [self.retrieve_original_file(parsed_key)
                            for parsed_key in self.list_parsed_correction_keys()]
        return list(set(files_to_correct))

    @staticmethod
    def get_local_file_content(source_path: str) -> str:
        try:
            with open(source_path, "r", encoding="utf-8") as file:
                content = file.read()
        except UnicodeDecodeError:
            with open(source_path, "r", encoding="latin-1") as file:
                content = file.read()
        return content

    @staticmethod
    def write_local_file(file_path: str, content: str):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(content)

    @staticmethod
    def write_local_file_from_list(file_path: str, content: list):
        with open(file_path, "w", encoding="utf-8") as file:
            for line in content:
                file.write(line + "\n")

    @staticmethod
    def add_to_local_file_from_list(file_path: str, content: list):
        with open(file_path, "a", encoding="utf-8") as file:
            for line in content:
                file.write(line + "\n")

    @abstractmethod
    def get_file_content(self, file_key: str) -> str:
        pass

    @abstractmethod
    def write_file(self, file_key: str, content: str):
        pass

    @abstractmethod
    def write_file_from_list(self, file_key: str, content: list):
        pass

    def correct_source_files(self):
        """
        Correct the corrupted files in the source directory
        """
        print("Correcting corrupted files in the source directory")
        files_dict = self.list_source_files_dict()
        corrupted_files = [file for file in files_dict if file.get("filename").startswith("summary")]
        print("List of corrupted files:")
        for file in corrupted_files:
            print(file.get("filename"))
        # Change the filename of the corrupted files
        for file in corrupted_files:
            new_filename = file.get("filename")[7:]
            base_path = os.path.join(file.get("root"), file.get("filename"))
            new_path = os.path.join(file.get("root"), new_filename)
            os.replace(base_path, new_path)
            print(f"File {base_path} renamed to {new_filename}")

    def correct_file_content(self, content_text: str) -> str:
        """
        Correct the content of a file
        """
        corrections_file_text = self.get_file_content(self.name_corrections_file_key)
        corrections_dict = json.loads(corrections_file_text)
        correction_patterns = {
                "\\u20ac": "€",
                "\\u2013": "–",
                "\\u00e9": "é"
        }
        corrections = correction_patterns | corrections_dict

        for old_value, new_value in corrections.items():
            content_text = content_text.replace(old_value, new_value)
        return content_text

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
        error_files_content = self.get_local_file_content(self.error_files_path)
        error_files = error_files_content.split()
        return error_files

    def add_to_error_files(self, source_key: str):
        """
        Add a filename to the error_files.txt file
        """
        self.add_to_local_file_from_list(self.error_files_path, [source_key])
        print(f"File {source_key} added to {self.error_files_path}")

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
        sorted_files_content = self.get_local_file_content(self.sorted_files_record_path)
        sorted_files = sorted_files_content.split()
        return sorted_files
    
    def add_to_sorted_files(self, source_key: str):
        """
        Add a filename to the sorted_files.txt file
        """
        self.add_to_local_file_from_list(self.sorted_files_record_path, [source_key])
        print(f"File {source_key} added to {self.sorted_files_record_path}")

    def reset_local_file(self, file_path: str):
        """
        Reset a local file
        """
        self.write_local_file(file_path, "")
        print(f"{file_path} reset successfully")

    def reset_file(self, file_key: str):
        """
        Reset a file in the S3 bucket
        """
        self.write_file(file_key, "")
        print(f"{file_key} reset successfully")

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
        self.merge_files()
        self.correct_source_files()
        self.reset_local_file(self.sorted_files_record_path)
        print("Sorting files from the source directory to the raw directory")
        files_to_sort = self.list_source_files_dict()[::-1]
        print(f"Number of files to sort: {len(files_to_sort)}")
        with ThreadPoolExecutor() as executor:
            future_to_file = {executor.submit(self.sort_file, file): file for file in files_to_sort}
            for future in as_completed(future_to_file):
                future.result()
        print("All files sorted successfully")

    def sort_files_to_correct(self):
        print("Sorting files with correction to do from the source directory to the raw directory")
        all_files = self.list_source_files_dict()
        files_to_sort = self.list_correction_original_files()[::-1]
        print(f"Number of files to sort: {len(files_to_sort)}")
        list_to_sort_dict = [file for file in all_files
                             if os.path.join(file["root"], file["filename"]) in files_to_sort]
        raw_keys = [self.get_raw_key(self.get_file_info(file).get("raw_key_suffix")) for file in list_to_sort_dict]
        self.write_file_from_list(self.correction_raw_keys_file_key, raw_keys)
        self.reset_file(self.names_to_correct_file_key)
        with ThreadPoolExecutor() as executor:
            future_to_file = {executor.submit(self.sort_file, file): file for file in list_to_sort_dict}
            for future in as_completed(future_to_file):
                future.result()
        for parsed_file in self.list_parsed_correction_keys():
            os.remove(parsed_file)
        print("All files sorted successfully")

    def sort_new_files(self):
        """
        Upload new files from the source directory to the file directory
        """
        self.merge_files()
        self.correct_source_files()
        print("Sorting new files from the source directory to the raw directory")
        files_to_sort = self.list_source_files_dict()[::-1]
        with ThreadPoolExecutor() as executor:
            future_to_file = {executor.submit(self.sort_file, file): file for file in files_to_sort}
            for future in as_completed(future_to_file):
                future.result()
        print("All new files sorted successfully")

    def list_files_to_merge(self):
        """
        List all the files that need to be joined
        """
        files = self.list_source_files_dict()
        file_info_list = [
            {
                "filename": file_dict.get("filename"),
                "source_key": self.get_file_info(file_dict).get("source_key"),
                "raw_key_suffix": self.get_info_from_filename(file_dict.get("filename")).get("raw_key_suffix"),
            }
            for file_dict in files
        ]
        raw_suffixes = [info.get("raw_key_suffix") for info in file_info_list]
        suffix_to_files_dict = {suffix: [info.get("source_key")
                                         for info in file_info_list
                                         if info.get("raw_key_suffix") == suffix]
                                for suffix in raw_suffixes}
        files_to_join = {suffix: source_keys
                         for suffix, source_keys in suffix_to_files_dict.items()
                         if len(source_keys) > 1}
        return files_to_join

    def merge_files(self):
        """
        Merge files with the same raw key, coming from list_files_to_join
        """
        print("Merging files with the same raw key in the source directory")
        files_to_merge = self.list_files_to_merge()
        print(f"Number of files to merge: {len(files_to_merge)}")
        print("List of files to merge:")
        for raw_key_suffix, source_keys in files_to_merge.items():
            print(f"Raw key suffix: {raw_key_suffix}")
            for source_key in source_keys:
                print(f"Source key: {source_key}")
        for source_keys in files_to_merge.values():
            ref_source_key = source_keys[0]
            with open(ref_source_key, "a", encoding="utf-8") as ref_file:
                for source_key in source_keys[1:]:
                    with open(source_key, "r", encoding="utf-8") as file:
                        ref_file.write(file.read())
                    os.remove(source_key)
                    print(f"File {source_key} merged with {ref_source_key}")

    def retrieve_original_file(self, parsed_key: str) -> str:
        """
        Retrieve the original file from the source directory
        """
        retrieval_pattern = re.compile(r"[\\/](\d{4})[\\/](\d{2})[\\/](\d{2})[\\/](\d+)[\\/]([\w\-]+).json")
        match = retrieval_pattern.search(parsed_key)
        tournament_id = match.group(4)
        return self.tournaments_dict[tournament_id]

    def find_problematic_player_names(self, source_key: str):
        """
        Find the names of the players that are not correctly parsed
        """
        file_content = self.get_local_file_content(source_key)
        parser_pattern = r"Seat \d+: ([\w\s.\-&\⌃]{3,12}) \((\d+)(?:, ([\d\.]+)\D)?"
        global_pattern = r"Seat \d+: ((?:(?!\n).)+) \((\d+)(?:, ([\d\.]+)\D)?"
        parser_names = set(data[0] for data in re.findall(parser_pattern, file_content))
        global_names = set(data[0] for data in re.findall(global_pattern, file_content))
        problematic_names = parser_names ^ global_names
        return problematic_names

    def save_problematic_names(self):
        """
        Save the names of the players that are not correctly parsed
        """
        print("Saving the names of the players that are not correctly parsed")
        source_keys = self.list_correction_original_files()
        names = [self.find_problematic_player_names(source_key) for source_key in source_keys]
        source_keys_set = set.union(*names) if names else set()
        problematic_names = list(source_keys_set)
        self.write_file_from_list(self.names_to_correct_file_key, problematic_names)

    def set_correction_names(self):
        """
        Set the names of the players that are not correctly parsed
        """
        print("Setting the names of the players that are not correctly parsed")
        names_to_correct_content = self.get_file_content(self.names_to_correct_file_key)
        names_to_correct = names_to_correct_content.split()
        name_corrections_content = self.get_file_content(self.name_corrections_file_key)
        name_corrections_dict = json.loads(name_corrections_content)
        nb_villains = len(name_corrections_dict)
        for i, name in enumerate(names_to_correct):
            if name not in name_corrections_dict.keys():
                name_corrections_dict[name] = f"Villain_{i+nb_villains:03}_"
        json.dump(name_corrections_dict, open(self.name_corrections_file_key, "w"), indent=4)

    def correct_files(self):
        self.save_problematic_names()
        self.set_correction_names()
        self.sort_files_to_correct()
