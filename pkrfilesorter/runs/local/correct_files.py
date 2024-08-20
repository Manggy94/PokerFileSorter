from pkrfilesorter.sorters.local import LocalFileSorter
from pkrfilesorter.settings import SOURCE_DIR, DATA_DIR


if __name__ == "__main__":
    sorter = LocalFileSorter(source_dir=SOURCE_DIR, local_data_dir=DATA_DIR)
    sorter.correct_files()
