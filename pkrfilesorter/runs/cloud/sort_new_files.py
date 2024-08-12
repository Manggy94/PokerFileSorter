from pkrfilesorter.sorters.cloud import CloudFileSorter
from pkrfilesorter.settings import BUCKET_NAME, DATA_DIR, SOURCE_DIR

if __name__ == "__main__":
    sorter = CloudFileSorter(source_dir=SOURCE_DIR, data_dir=DATA_DIR, bucket_name=BUCKET_NAME)
    sorter.sort_new_files()
