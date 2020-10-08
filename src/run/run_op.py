from src.data_catalog.catalog import CATALOG
from src.extract.extract import extract_data
from src.load.load import data_to_db
from src.run.run_utils import delete_tmp



def op_etl(raw_location: str, db_table: str) -> None:
    data = extract_data(
        CATALOG["HES-OP"]["s3_raw"][raw_location], 
        CATALOG["HES-OP"]["local_raw"][f"{raw_location}_zip"],
        CATALOG["HES-OP"]["local_raw"][raw_location],
        "op")
    data_to_db(data, CATALOG["HES-OP"]["rds"][db_table])
    delete_tmp()


def main():

    op_etl("2014", "op")
    op_etl("2015", "op")
    op_etl("2016", "op")
    op_etl("2017", "op")
    op_etl("2018", "op")


if __name__ == "__main__":
    main()