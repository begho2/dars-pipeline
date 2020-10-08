from src.data_catalog.catalog import CATALOG
from src.extract.extract import extract_data
from src.load.load import data_to_db
from src.run.run_utils import delete_tmp


def cc_etl(raw_location: str, db_table: str) -> None:
    data = extract_data(
        CATALOG["HES-CC"]["s3_raw"][raw_location], 
        CATALOG["HES-CC"]["local_raw"][f"{raw_location}_zip"],
        CATALOG["HES-CC"]["local_raw"][raw_location],
        "cc")
    data_to_db(data, CATALOG["HES-CC"]["rds"][db_table])
    delete_tmp()


def main():

    cc_etl("2014", "cc")
    cc_etl("2015", "cc")
    cc_etl("2016", "cc")
    cc_etl("2017", "cc")
    cc_etl("2018", "cc")


if __name__ == "__main__":
    main()