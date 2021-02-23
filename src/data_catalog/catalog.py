"""
Provide a simple data catalog to act as
the single point of truth for the location of
data.

This catalog assumes the data lake is filesystem based.
In realistic situations, it does not have to be.

TODO: improve by making an abstraction of the
 type of the data (database, file: csv/parquet/…, …)
"""

def _s3_location(zone: str, key: str) -> str:
    return f"{zone}/{key}"


def _db_table(schema: str, table: str) -> str:
    return f"{schema}.{table}"


CATALOG = {
    "HES-AE":{
        "s3": {
            "2014": _s3_location("HES-AE", "NIC243790_HES_AE_201499.zip"),
            "2015": _s3_location("HES-AE", "NIC243790_HES_AE_201599.zip"),
            "2016": _s3_location("HES-AE", "NIC243790_HES_AE_201699.zip"),
            "2017": _s3_location("HES-AE", "NIC243790_HES_AE_201799.zip"),
            "2018": _s3_location("HES-AE", "NIC243790_HES_AE_201899.zip"),
        },
        "rds": {
            "ae": _db_table("hes", "ae")
        },
    },
    "HES-APC": {
        "s3": {
            "2014": _s3_location("HES-APC", "NIC243790_HES_APC_201499.zip"),
            "2015": _s3_location("HES-APC", "NIC243790_HES_APC_201599.zip"),
            "2016": _s3_location("HES-APC", "NIC243790_HES_APC_201699.zip"),
            "2017": _s3_location("HES-APC", "NIC243790_HES_APC_201799.zip"),
            "2018": _s3_location("HES-APC", "NIC243790_HES_APC_201899.zip"),
            "2019": _s3_location("HES-APC", "NIC243790_HES_APC_201912.zip"),
            "2020": _s3_location("HES-APC", "NIC243790_HES_APC_202004.zip"),
        },
        "rds": {
            "apc": _db_table("hes", "apc")
        },
    },
    "HES-OP": {
        "s3": {
            "2014": _s3_location("HES-OP", "NIC243790_HES_OP_201499.zip"),
            "2015": _s3_location("HES-OP", "NIC243790_HES_OP_201599.zip"),
            "2016": _s3_location("HES-OP", "NIC243790_HES_OP_201699.zip"),
            "2017": _s3_location("HES-OP", "NIC243790_HES_OP_201799.zip"),
            "2018": _s3_location("HES-OP", "NIC243790_HES_OP_201899.zip"),
            "2019": _s3_location("HES-OP", "NIC243790_HES_OP_201912.zip"),
            "2020": _s3_location("HES-OP", "NIC243790_HES_OP_202004.zip"),
        },
        "rds": {
            "op": _db_table("hes", "op")
        },
    },
    "HES-ECDS": {
        "s3": {
            
            "2019": _s3_location("HES-ECDS", "NIC243790_HES_ECDS_201999.zip"),
            "2020": _s3_location("HES-ECDS", "NIC243790_HES_ECDS_202008.zip"),
        },
        "rds": {
            "2019": _db_table("hes", "ecds_2019"),
            "2020": _db_table("hes", "ecds_2020"),
            "ecds": _db_table("hes", "ecds")
        },
    }
}
