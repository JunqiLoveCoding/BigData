import json
import os

from dateutil import parser
from pyspark.shell import sc
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from enum import Enum
import time
import sys

def main(start_index, end_index):
    spark = SparkSession \
        .builder \
        .appName("big_data_prof") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    spark_context = spark.sparkContext
    hadoop = spark_context._jvm.org.apache.hadoop
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path('/user/hm74/NYCOpenData')
    total_files = len(fs.get(conf).listStatus(path)[start_index:end_index])
    os.mkdir('job_{}_{}'.format(start_index, end_index))
    for i, nyc_open_datafile in enumerate(fs.get(conf).listStatus(path)[start_index:end_index]):
        print("processing {} of {}".format(i, total_files))
        # pretty hacky preprocessing but it will work for now
        # could maybe use pathlib library or get it with hdfs
        processed_path = str(nyc_open_datafile.getPath()).replace("hdfs://dumbo", "")
        df_nod = spark.read.option("header", "true").option("delimiter", "\t").csv(processed_path)
        try:
            file_name = processed_path.split('/')[-1].replace('.tsv.gz', '')
            print(file_name)
            start_process = time.time()
            bp = BasicProfiling(processed_path, df_nod)
            table_dict = bp.process()
            json_type = json.dumps(table_dict)
            with open("job_{}_{}/{}.json".format(start_index, end_index, file_name), 'w+') as f:
                f.write(json_type)
            end_process = time.time()
            print("total process time {}".format(end_process - start_process))
        except Exception as e:
            print("unable to process because {}".format(e))


# We should put this in it's on package, but submitting with packages is kind of annoying so
# I moved it out for now look at --py-files
#https://spark.apache.org/docs/latest/submitting-applications.html
class BasicProfiling:
    """
    Class for data profiling basic schema and statistics on a dataframe
    """
    def __init__(self, dataset_name, df_nod):
        self.dataset_name = dataset_name
        self.df_nod = df_nod
        self.table_dict = dict()
        self.columns = self.df_nod.columns
        self.spec_types = ['INT', 'REAL', 'DATE', 'TEXT']
        # self.column_dict = None # the currently processed column dict
        # self.column = None # the currently processed column dataframe

    def __set_up_dictionary(self):
        self.table_dict['dataset_name'] = self.dataset_name
        self.table_dict['columns'] = []

    def __add_column_general_info(self, column, column_name):
        general_count = column.select(lit(column_name).alias("name"), count(column_name).alias("count"), countDistinct(column_name).alias("distinct"))
        general_empty = column.filter(col(column_name).isNull()).fillna({column_name: '0'}).select(count(column_name).alias("empty"))
        general_fre = column.groupBy(column_name).agg(count(column_name).alias("count")).orderBy(desc("count")).limit(5).agg(collect_list(column_name).alias('fre'))
        return general_count, general_empty, general_fre

    def _add_datatype_columns(self, column, column_name):
        """
        Adds a type column to add every column we currently have, obviously this doubles the size
        :return:
        """
        get_column_type_udf = udf(self.get_column_type)
        column = column.withColumn("dtype", get_column_type_udf(column_name))
        return column

    def __get_stats_int(self, column, column_name):
        int_info = column.filter("dtype = 'INT'").withColumn(column_name, column[column_name].cast('int'))\
            .select(array(count(column_name), max(column_name), min(column_name), mean(column_name), stddev(column_name)).alias('stats_int'))
        return int_info

    def __get_stats_double(self, column, column_name):
        double_info = column.filter("dtype = 'REAL'").withColumn(column_name, column[column_name].cast('double')).\
            select(array(count(column_name), max(column_name), min(column_name), mean(column_name), stddev(column_name)).alias('stats_double'))
        return double_info

    def __get_stats_date(self, column, column_name):
        date_info = column.filter("dtype = 'DATE'")\
            .select(array(count(column_name), max(column_name), min(column_name)).alias('stats_date'))
        return date_info

    def __get_stats_text(self, column, column_name):
        df_len = column.filter("dtype = 'TEXT'").withColumn("len", length(column_name))
        text_info = df_len.select(array(count(column_name), mean("len")).alias('stats_text'))
        shortest = df_len.orderBy(asc("len")).limit(5).agg(collect_list(column_name).alias('shortest_values')).select('shortest_values')
        longest = df_len.orderBy(desc("len")).limit(5).agg(collect_list(column_name).alias('longest_values')).select('longest_values')
        return text_info, shortest, longest

    def __convert_df_to_dict(self, integer, real, date, text, shortest, longest, count, empty, fre):
        stats_int = integer.collect()
        stats_double = real.collect()
        stats_date = date.collect()
        stats_text = text.collect()
        stats_shortest = shortest.collect()
        stats_longest = longest.collect()
        general_count = count.collect()
        general_empty = empty.collect()
        general_fre = fre.collect()
        for i in range(len(stats_int)):
            column_dict = {}
            column_stats = [general_count[i][0], stats_int[i][0], stats_double[i][0], stats_date[i][0], stats_text[i][0], stats_shortest[i][0], stats_longest[i][0]]
            column_dict['column_name'] = column_stats[0]
            column_dict['number_empty_cells'] = general_empty[i][0]
            column_dict['number_non_empty_cells'] = general_count[i][1]
            column_dict['number_distinct_values'] = general_count[i][2]
            column_dict['frequent_values'] = general_fre[i][0]
            column_dict['data_type'] = []
            if column_stats[1][0] != 0:
                type_dict = {}
                type_dict['type'] = "INTERGER(LONG)"
                type_dict['count'] = int(column_stats[1][0])
                type_dict['max_value'] = int(column_stats[1][1])
                type_dict['min_value'] = int(column_stats[1][2])
                type_dict['mean'] = float(column_stats[1][3])
                type_dict['stddev'] = float(column_stats[1][4])
                column_dict['data_type'].append(type_dict)
            if column_stats[2][0] != 0:
                type_dict = {}
                type_dict['type'] = 'REAL'
                type_dict['count'] = int(column_stats[2][0])
                type_dict['max_value'] = float(column_stats[2][1])
                type_dict['min_value'] = float(column_stats[2][2])
                type_dict['mean'] = float(column_stats[2][3])
                type_dict['stddev'] = float(column_stats[2][4])
                column_dict['data_type'].append(type_dict)
            if column_stats[3][0] != '0':
                type_dict = {}
                type_dict['type'] = "DATE/TIME"
                type_dict['count'] = int(column_stats[3][0])
                type_dict['max_value'] = column_stats[3][1]
                type_dict['min_value'] = column_stats[3][2]
                column_dict['data_type'].append(type_dict)
            if column_stats[4][0] != 0:
                type_dict = {}
                type_dict['type'] = "TEXT"
                type_dict['count'] = column_stats[4][0]
                type_dict['shortest_value'] = column_stats[5][0]
                type_dict['longest_value'] = column_stats[6][0]
                type_dict['average_length'] = column_stats[4][1]
                column_dict['data_type'].append(type_dict)
            self.table_dict['columns'].append(column_dict)

    @staticmethod
    def get_column_type(val):
        """
        Returns the type of the value
        :param val:
        :return:
        """
        if BasicProfiling.__is_int(val):
            return 'INT'
        elif BasicProfiling.__is_real(val):
            return 'REAL'
        elif BasicProfiling.__is_datetime(val):
            return 'DATE'
        elif val is None:
            return None
        else:
            return 'TEXT'

    @staticmethod
    def __is_int(val):
        try:
            int(val)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def __is_real(val):
        try:
            float(val)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def __is_datetime(val):
        try:
            parser.parse(val)
            return True
        # raw exception here, I tried to catch none raw dateutil error exception, but it's giving some errors
        # not sure I will need to fix up.
        except:
            return False

    def process(self):
        start = time.time()
        self.__set_up_dictionary()

        for i, column_name in enumerate(self.columns):
            column = self.df_nod.select(column_name)

            general_count, general_empty, general_fre = self.__add_column_general_info(column, column_name)

            # # generate type_dict
            column = self._add_datatype_columns(column, column_name)

            stats_int = self.__get_stats_int(column, column_name)

            stats_double = self.__get_stats_double(column, column_name)

            stats_date = self.__get_stats_date(column, column_name)

            stats_text, shortest, longest = self.__get_stats_text(column, column_name)

            if i == 0:
                stats_table_int = stats_int
                stats_table_double = stats_double
                stats_table_date = stats_date
                stats_table_text = stats_text
                table_shortest = shortest
                table_longest = longest
                general_table_count = general_count
                general_table_empty = general_empty
                general_table_fre = general_fre
            else:
                stats_table_int = stats_table_int.union(stats_int)
                stats_table_double = stats_table_double.union(stats_double)
                stats_table_date = stats_table_date.union(stats_date)
                stats_table_text = stats_table_text.union(stats_text)
                table_shortest = table_shortest.union(shortest)
                table_longest = table_longest.union(longest)
                general_table_count = general_table_count.union(general_count)
                general_table_empty = general_table_empty.union(general_empty)
                general_table_fre = general_table_fre.union(general_fre)

        self.__convert_df_to_dict(stats_table_int, stats_table_double, stats_table_date, stats_table_text, table_shortest, table_longest, general_table_count, general_table_empty, general_table_fre)
        return self.table_dict

# Seems like there is a bug in pyspark when serializing enum class, will leave it in for now.
# https://stackoverflow.com/questions/58071115/dict-object-has-no-attribute-member-names-problem-with-enum-class-while-us
# class SpecType(Enum):
#     INT = "INT"
#     REAL = "REAL"
#     DATE = "DATE"
#     TEXT = "TEXT"


if __name__ == "__main__":
    start_index = int(sys.argv[1])
    end_index = int(sys.argv[2])
    main(start_index, end_index)
