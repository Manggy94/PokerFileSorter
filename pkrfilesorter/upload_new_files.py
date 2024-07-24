"""This module is the 2nd entry point of the application.
It creates an instance of S3FileSorter and calls the upload_files method to upload files to an S3 bucket.
"""
from pkrfilesorter.s3_file_sorter import S3FileSorter
from pkrfilesorter.config import SOURCE_DIR, BUCKET_NAME, DATA_DIR


if __name__ == "__main__":
    print(f"Uploading new files from '{SOURCE_DIR}' to '{BUCKET_NAME}'")
    file_sorter = S3FileSorter(SOURCE_DIR, BUCKET_NAME, DATA_DIR)
    print("File sorter initialized successfully")
    try:
        file_sorter.upload_files()
        print("Files copied successfully")
    except Exception as e:
        print(f"An error occurred: {e}")

