import dateutil.parser
import operator


class BasicProfiling:
    """
    Class for missing column types
    """
    def __init__(self, df_nod, data_type_threshold=None):
        self.df_nod = df_nod
        self.type_dict = dict()
        if data_type_threshold:
            self.dtt = data_type_threshold
        else:
            self.dtt = .5

    def find_all_column_types(self):
        """
        Finds all the types for all the columns
        :return:
        """
        self.__find_using_spark()
        missing_col_types = self.__get_missing_col_types()
        if missing_col_types:
            self.__find_manually(missing_col_types)
        return self.type_dict

    def __find_using_spark(self):
        """
        Tries to use spark to find the types
        :return:
        """
        for column_name, column_type in self.df_nod.dtypes:
            if column_type != "string":
                self.type_dict[column_name] = column_type

    def __find_manually(self, missing_column_types):
        """
        Goes row by row to find the types
        :return:
        """
        total_rows = self.df_nod.count()
        # prepopulate dict
        count_dict = dict()
        for column in missing_column_types:
            count_dict[column] = {'int': 0, 'real': 0, 'date': 0}
        # We can do this without having to loop through all the rows
        # We can do each column, we can turn all the strings into null and then apply the int.
        # This can be less performant since we have to do it on a column by column basis, if we have time we should
        # profile the performance.
        # https://stackoverflow.com/questions/42709279/casting-string-to-int-null-issue
        for row in self.df_nod.rdd.collect():
            for column in missing_column_types:
                process_val = row[column]
                if self.__is_int(process_val):
                    count_dict[column]['int'] = count_dict[column]['int'] + 1
                if self.__is_real(process_val):
                    count_dict[column]['real'] = count_dict[column]['real'] + 1
                if self.__is_datetime(process_val):
                    count_dict[column]['date'] = count_dict[column]['date'] + 1
        for column in missing_column_types:
            count_name, count_value = max(count_dict[column].items(), key=operator.itemgetter(1))
            if count_value/total_rows > self.dtt:
                # floats are always ints so we need to test to make sure we shouldn't make ints
                if count_name == 'real':
                    if count_value == count_dict[column]['int']:
                        count_name = 'int'
                self.type_dict[column] = count_name
            else:
                self.type_dict[column] = 'string'

    def __get_missing_col_types(self):
        """
        Checks if there are any types missing
        :return:
        """
        missing_types = []
        for column in self.df_nod.columns:
            if not (self.type_dict.get(column)):
                missing_types.append(column)
        return missing_types

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
            dateutil.parser.parse(val)
            return True
        except (dateutil.parser._parser.ParserError, TypeError):
            return False
