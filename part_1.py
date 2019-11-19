from dateutil import parser
from pyspark.shell import sc
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from enum import Enum

def main():
    spark = SparkSession \
        .builder \
        .appName("big_data_prof") \
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
        df_nod = spark.read.option("header", "true").option("delimiter", "\t").csv(processed_path)
        bp = BasicProfiling(processed_path, df_nod)
        type_dict = bp.process()
        print(type_dict)


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

    def __add_column_general_info(self, column, column_dict):
        total_num = column.count()
        column_dict['number_empty_cells'] = column.rdd.filter(lambda x: x.isNull()).count()
        column_dict['number_non_empty_cells'] = total_num - column_dict['number_empty_cells']
        column_dict['number_distinct_values'] = column.distinct().count()
        column_dict['frequent_values'] = column.groupBy("name").count().orderBy(desc('count')).limit(5).select("name").rdd.flatMap(list).collect()
        return column_dict

    def _add_datatype_columns(self, column):
        """
        Adds a type column to add every column we currently have, obviously this doubles the size
        :return:
        """
        get_column_type_udf = udf(self.get_column_type)
        column = column.withColumn("dtype", get_column_type_udf("name"))
        return column

    def __add_stats_to_column_dict(self, column, column_dict, spec_type):
        """
        Adding count, min, max, etc. to the specification of each column in the dataframe.
        :return:
        """
        type_dict = {}
        if spec_type == 'INT':
            type_dict['type'] = "INTERGER(LONG)"
            stats = column.filter("dtype = 'INT'").withColumn("name", column.name.cast('int')).select(countDistinct("name"), max("name"), min("name"), mean("name"), stddev("name")).collect()
            type_dict['count'] = int(stats[0][0])
            type_dict['max_value'] = int(stats[0][1])
            type_dict['min_value'] = int(stats[0][2])
            type_dict['mean'] = float(stats[0][3])
            type_dict['stddev'] = float(stats[0][4])
        elif spec_type == 'REAL':
            type_dict['type'] = 'REAL'
            stats = column.filter("dtype = 'REAL'").withColumn("name", column.name.cast('double')).select(countDistinct("name"), max("name"), min("name"), mean("name"), stddev("name")).collect()
            type_dict['count'] = int(stats[0][0])
            type_dict['max_value'] = float(stats[0][1])
            type_dict['min_value'] = float(stats[0][2])
            type_dict['mean'] = float(stats[0][3])
            type_dict['stddev'] = float(stats[0][4])
        elif spec_type == 'DATE':
            type_dict['type'] = "DATE/TIME"
            stats = column.filter("dtype = 'DATE'").select(countDistinct("name"), max("name"), min("name")).collect()
            type_dict['count'] = int(stats[0][0])
            type_dict['max_value'] = stats[0][1]
            type_dict['min_value'] = stats[0][2]
        else:
            type_dict['type'] = "TEXT"
            stats = column.withColumn("len", length("name"))
            type_dict['count'] = stats.select("name").distinct().count()
            type_dict['shortest_value'] = stats.orderBy(asc("len")).limit(5).select("name").rdd.map(lambda x: x[0]).collect()
            type_dict['longest_value'] = stats.orderBy(desc("len")).limit(5).select("name").rdd.map(lambda x: x[0]).collect()
            type_dict['average_length'] = stats.select(mean("len")).collect()[0][0]
         
        return type_dict

    # def get_column_spec_dict(self, column_name):
    #     for column_dict in self.table_dict['column_specification']:
    #         if column_dict['column_name'] == column_name:
    #             return column_dict

    # def get_column_spec_type_dict(self, column_name, column_type):
    #     column_name_dict = self.get_column_spec_dict(column_name)
    #     for column_type_spec_dict in column_name_dict['data_types']:
    #         if column_type_spec_dict['type'] == column_type:
    #             return column_type_spec_dict

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
        self.__set_up_dictionary()
        print(self.columns)
        for column in self.columns:
            print(column)
            column_dict = {}
            column_dict['column_name'] = column
            # select the currently processed column and rename it as "name"
            print("The process column is {}".format(column))
            column = self.df_nod.select(col(column).alias("name"))
            column_dict = self.__add_column_general_info(column, column_dict)

            # generate type_dict
            column_dict['data_type'] = [] 
            column = self._add_datatype_columns(column)
            types = column.select("dtype").distinct().collect()[:][0]
            for spec_type in types:
                type_dict = self.__add_stats_to_column_dict(column, column_dict, spec_type)
                column_dict['data_type'].append(type_dict)

            print(column_dict)
            self.table_dict['columns'].append(column_dict)
                
        return self.table_dict

# Seems like there is a bug in pyspark when serializing enum class, will leave it in for now.
# https://stackoverflow.com/questions/58071115/dict-object-has-no-attribute-member-names-problem-with-enum-class-while-us
# class SpecType(Enum):
#     INT = "INT"
#     REAL = "REAL"
#     DATE = "DATE"
#     TEXT = "TEXT"


if __name__ == "__main__":
    main()
