"""This config file contains the source and destination directories for the file sorter."""
import os
from dotenv import load_dotenv

load_dotenv()

SOURCE_DIR = os.environ.get("SOURCE_DIR")
DATA_DIR = os.getenv("DATA_DIR")
BUCKET_NAME = os.getenv("BUCKET_NAME")

if __name__ == "__main__":
    print(SOURCE_DIR)
    print(DATA_DIR)