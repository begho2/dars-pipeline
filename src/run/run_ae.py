from src.data_catalog.catalog import CATALOG
from src.extract.extract import extract_data, extract_unzipped_data
from src.transform.clean_ae import make_boolean, remove_null
from src.load.load import data_to_parquet, data_to_db
from src.run.run_utils import delete_tmp

from src.config import setup_logging
import logging

setup_logging()
logger = logging.getLogger(__name__)


def ae_etl(raw_location: str, db_table: str) -> None:
    logger.info("Running ae_etl")
    # data = extract_data(
    #     CATALOG["HES-AE"]["s3_raw"][raw_location],
    #     CATALOG["HES-AE"]["local_raw"][f"{raw_location}_zip"],
    #     CATALOG["HES-AE"]["local_raw"][raw_location],
    #     "ae")
    data = extract_unzipped_data(CATALOG["HES-AE"]["local_raw"][raw_location])
    logger.info("AE data extracted")
    data_clean = make_boolean(data, "NEWNHSNO_CHECK")
    logger.info("AE data cleaned")
    # data_to_db(data_clean, CATALOG["HES-AE"]["rds"][db_table])
    logger.info("AE data stored in DB")
    # delete_tmp()
    logger.info("AE data cleared")


def main():

    ae_etl("2014_trunc", "ae")
    # ae_etl("2015", "ae")
    # ae_etl("2016", "ae")
    # ae_etl("2017", "ae")
    # ae_etl("2018", "ae")


if __name__ == "__main__":
    main()