"""This config file contains the source and destination directories for the file sorter."""
import os
from dotenv import load_dotenv

load_dotenv()

SOURCE_DIR = os.environ.get("SOURCE_DIR")
DESTINATION_DIR = os.getenv("DESTINATION_DIR")

if __name__ == "__main__":
    print(SOURCE_DIR)
    print(DESTINATION_DIR)