"""This module is the 2nd entry point of the application.
It creates an instance of S3FileSorter and calls the upload_files method to upload files to an S3 bucket.
"""
from pkrfilesorter.s3_file_sorter import S3FileSorter
from pkrfilesorter.config import SOURCE_DIR, DESTINATION_BUCKET


if __name__ == "__main__":
    print(f"Uploading files from '{SOURCE_DIR}' to '{DESTINATION_BUCKET}'")
    try:
        file_sorter = S3FileSorter(SOURCE_DIR, DESTINATION_BUCKET)
        print("File sorter initialized successfully")
        file_sorter.correct_source_files()
        file_sorter.upload_files()
        print("Files copied successfully")
    except Exception as e:
        print(f"An error occurred: {e}")
