"""This module is the 2nd entry point of the application.
It creates an instance of S3FileSorter and calls the upload_files method to upload files to an S3 bucket.
"""
from pkrfilesorter.local_file_sorter import LocalFileSorter
from pkrfilesorter.config import SOURCE_DIR, DATA_DIR


if __name__ == "__main__":
    print(f"Copying new files from '{SOURCE_DIR}' to '{DATA_DIR}'")
    file_sorter = LocalFileSorter(SOURCE_DIR, data_dir=DATA_DIR)
    print("File sorter initialized successfully")
    #try:
    file_sorter.copy_new_files()
    print("Files copied successfully")
    # except Exception as e:
    #     print(f"An error occurred: {e}")

