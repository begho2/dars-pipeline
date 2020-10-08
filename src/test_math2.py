from zipfile import ZipFile, ZIP_LZMA, ZIP_DEFLATED

from pyspark import SparkConf

from mymod.math2 import square_it
from pjbmath import double_it

def test_square_it():
    assert 9 == square_it(3)

def test_double_it():
    assert 4 == double_it(2)


def test_pyspark():
    from pyspark.sql import SparkSession
    conf = SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("PJB Test")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
        # .master("local[2]")
        # .config(conf=SPARK_CONFIG) \

    # frame = (
    #     spark.read.options(header=True, delimiter=',', inferSchema=True)
    #         .csv(raw_location)
    #         # .schema(schema) \
    #         # .csv(CATALOG["local_raw/hes_ae_2014"])
    # )
    # frame = frame.toDF(*[c.lower() for c in frame.columns])
    # frame.show(n=5)

def test_build_csv():
    import random
    import uuid
    outfile = 'data.csv'
    outsize = 1024 * 1024 * 2  # 2MB
    with open(outfile, 'w') as csvfile:
        size = 0
        count = 0
        csvfile.write('SomeRand, Blah1, Val1, Val2\n')
        while size < outsize:
            txt = '%s,%.6f,%.6f,%i\n' % (
                uuid.uuid4(), random.random() * 50, random.random() * 50, random.randrange(1000))
            size += len(txt)
            count += 1
            csvfile.write(txt)

    print(f'counted {count} rows')
    zip = ZipFile('data.zip','w',compression=ZIP_DEFLATED)
    zip.write(outfile)
    zip.close()
    assert 0 == 0
