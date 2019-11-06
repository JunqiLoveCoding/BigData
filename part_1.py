from pyspark.shell import sc
from pyspark.sql import SparkSession

from core.basic_profiling import BasicProfiling


def main():
    spark = SparkSession \
        .builder \
        .appName("hw2sql") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    spark_context = spark.sparkContext
    hadoop = spark_context._jvm.org.apache.hadoop

    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path('/user/hm74/NYCOpenData')

    for nyc_open_datafile in fs.get(conf).listStatus(path)[0:2]:
        # pretty hacky preprocessing but it will work for now
        # could maybe use pathlib library or get it with hdfs
        processed_path = str(nyc_open_datafile.getPath()).replace("hdfs://dumbo", "")
        print(processed_path)
        df_nod = spark.read.option("header", "true").option("delimiter", "\t").csv(processed_path)
        bp = BasicProfiling(df_nod)
        type_dict = bp.find_all_column_types()
        print(type_dict)


if __name__ == "__main__":
    main()