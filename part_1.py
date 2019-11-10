from dateutil import parser
from pyspark.shell import sc
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
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

    def __set_up_dictionary(self):
        self.table_dict['dataset_name'] = self.dataset_name
        self.table_dict['columns'] = self.columns
        self.table_dict['column_specification'] = []
        for column in self.columns:
            column_dict = dict()
            column_dict['column_name'] = column
            column_dict['data_types'] = []
            for spec_type in self.spec_types:
                column_type_dict = dict()
                column_type_dict['type'] = spec_type
                column_dict['data_types'].append(column_type_dict)
            self.table_dict['column_specification'].append(column_dict)

    def _add_datatype_columns(self):
        """
        Adds a type column to add every column we currently have, obviously this doubles the size
        :return:
        """
        get_column_type_udf = udf(self.get_column_type)
        for column in self.columns:
            self.df_nod = self.df_nod.withColumn("type_{}".format(column), get_column_type_udf(column))

    def __add_counts_to_table_dict(self):
        """
        Adding counts to the specification of each column in the dataframe.
        :return:
        """
        for column in self.columns:
            for spec_type in self.spec_types:
                type_dict = self.get_column_spec_type_dict(column, spec_type)
                # I'm not a huge fan of using the type column I think it's really cumbersome to use
                type_dict['count'] = self.df_nod.where(col("type_{}".format(column)) == spec_type).count()

    def get_column_spec_dict(self, column_name):
        for column_dict in self.table_dict['column_specification']:
            if column_dict['column_name'] == column_name:
                return column_dict

    def get_column_spec_type_dict(self, column_name, column_type):
        column_name_dict = self.get_column_spec_dict(column_name)
        for column_type_spec_dict in column_name_dict['data_types']:
            if column_type_spec_dict['type'] == column_type:
                return column_type_spec_dict

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
        self._add_datatype_columns()
        self.__add_counts_to_table_dict()
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