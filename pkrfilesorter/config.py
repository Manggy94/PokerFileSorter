"""This config file contains the source and destination directories for the file sorter."""
import os
from dotenv import load_dotenv

load_dotenv()

SOURCE_DIR = os.getenv("SOURCE_DIR")
DESTINATION_DIR = os.getenv("DESTINATION_DIR")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
DESTINATION_BUCKET = os.getenv("AWS_BUCKET_NAME")

if __name__ == "__main__":
    print(SOURCE_DIR)
    print(DESTINATION_DIR)