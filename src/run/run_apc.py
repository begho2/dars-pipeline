from src.data_catalog.catalog import CATALOG
from src.extract.extract import extract_data
from src.load.load import data_to_db
from src.run.run_utils import delete_tmp



def apc_etl(raw_location: str, db_table: str) -> None:
    data = extract_data(
        CATALOG["HES-APC"]["s3_raw"][raw_location], 
        CATALOG["HES-APC"]["local_raw"][f"{raw_location}_zip"],
        CATALOG["HES-APC"]["local_raw"][raw_location],
        "apc")
    data_to_db(data, CATALOG["HES-APC"]["rds"][db_table])
    delete_tmp()


def main():

    apc_etl("2014", "apc")
    apc_etl("2015", "apc")
    apc_etl("2016", "apc")
    apc_etl("2017", "apc")
    apc_etl("2018", "apc")


if __name__ == "__main__":
    main()