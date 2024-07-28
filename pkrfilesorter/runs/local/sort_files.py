from pkrfilesorter.sorters.local import LocalFileSorter
from pkrfilesorter.settings import SOURCE_DIR, DATA_DIR

if __name__ == "__main__":
    sorter = LocalFileSorter(SOURCE_DIR, DATA_DIR)
    sorter.sort_files()
