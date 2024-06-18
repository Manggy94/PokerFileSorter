"""This module is the entry point of the application.
It creates an instance of the FileSorter class and calls the copy_files method to copy files from the source directory
to the destination directory.
"""
from pkrfilesorter.file_sorter import FileSorter
from pkrfilesorter.config import SOURCE_DIR, DESTINATION_DIR


if __name__ == "__main__":
    file_sorter = FileSorter(SOURCE_DIR, DESTINATION_DIR)
    file_sorter.copy_files()