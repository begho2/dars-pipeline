from zipfile import ZipFile, ZIP_LZMA

# experiments in trying to efficiently load zip and transform from python


from pyspark import SparkConf

from mymod.math2 import square_it
from pjbmath import double_it

def stest_pyspark():
    from pyspark.sql import SparkSession
    conf = SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("PJB Test")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # spark.read.format("binaryFile").option("pathGlobFilter", "*.csv").load("../src/data.zip")



    data_files = spark.sparkContext.binaryFiles('../src/data.zip', 1000)
    file = data_files.first() #only one zip being loaded
    import io
    import zipfile
    in_memory_data = io.BytesIO(file[1]) # [0] is the file name. [1] is bytes representing data and metadata
    file_obj = zipfile.ZipFile(in_memory_data, "r") #create zip object representing data.zip? what have we actually achieved?
    file_obj2 = zipfile.ZipFile('../src/data.zip', "r") #read zip directly

    output_file= "new_out.csv"
    csv_filename = file_obj.namelist()[0]
    file_obj.extract(csv_filename, "new_out")
    df = spark.read.option("header", "true").csv(f'{output_file}/{csv_filename}' )
    df.show(100)

    # data_stream = file_obj.open(file).read()
    #
    #
    # y(file)
    # print(file)

def y(x):
    import io
    import zipfile
    in_memory_data = io.BytesIO(x[1])
    file_obj = zipfile.ZipFile(in_memory_data, "r")
    files = [i for i in file_obj.namelist()]
    return dict(zip(files, [file_obj.open(file).read() for file in files]))


# def try_again():
    # filenameRdd = sc.binaryFiles('hdfs://nameservice1:8020/user/*.binary')

def read_array(rdd):
    # output=zlib.decompress((bytes(rdd[1])),15+32) # in case also zipped
    from pandas import np
    from pyspark.sql.types import dt
    array = np.frombuffer(bytes(rdd[1])[20:], dtype=dt)  # remove Header (20 bytes)
    array = array.newbyteorder().byteswap()  # big Endian
    return array.tolist()

# unzipped = filenameRdd.flatMap(read_array)
# bin_df = sqlContext.createDataFrame(unzipped, schema)


def test_convert():
    import sys
    import os
    from zipfile import ZipFile
    import tarfile
    import gzip
    import time

    ifn = "../src/data.zip"
    ofn = "data.switched.tar.bz2"
    ofn_gzip = "data.switched.gzip"
    with ZipFile(ifn) as zipf:
        with gzip.open(ofn_gzip, 'wb') as gzipf:
            for zip_info in zipf.infolist():
                # print zip_info.filename, zip_info.file_size
                # gzip_info = gzip.GzipFile(filename=zip_info.filename)
                # gzip_info.size = zip_info.file_size
                # tar_info.mtime = time.mktime(list(zip_info.date_time) +
                #
                # gzipf.writelines(zipf.open(zip_info.filename).readlines())
                import shutil
                shutil.copyfileobj(zipf.open(zip_info.filename), gzipf)
                gzipf.close()

                # gzip.GzipFile(zip_info.filename,fileobj=zipf.open(zip_info.filename), mode='w')


                # gzipf.write(zipf.open(zip_info.filename))
                # gzipf.addfile(
                #     gzipinfo=gzip_info,
                #     fileobj=zipf.open(zip_info.filename)
                # )
        with tarfile.open(ofn, 'w:bz2') as tarf:
            for zip_info in zipf.infolist():
                # print zip_info.filename, zip_info.file_size
                tar_info = tarfile.TarInfo(name=zip_info.filename)
                tar_info.size = zip_info.file_size
                # tar_info.mtime = time.mktime(list(zip_info.date_time) +
                #                              [-1, -1, -1])
                tarf.addfile(
                    tarinfo=tar_info,
                    fileobj=zipf.open(zip_info.filename)
                )
    print (ofn)