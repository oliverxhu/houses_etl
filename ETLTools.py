import pandas as pd
from sqlalchemy import create_engine, exc  # exc contains all exceptions of SQLAlchemy
from sqlalchemy import Table, MetaData
import datetime
import psycopg2
import pygrametl
import json

class ETLTools:

    def flatten_json(self, y):
        out = {}

        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '_')
                    i += 1
            else:
                out[name[:-1]] = x

        flatten(y)
        return out

    def trim_dataframe(self, df, columns):
        """
        :param df: pandas DataFrame 
        :param columns: LIST of columns to trim
        :return: df
        """
        for column in columns:
            df[column] = df[column].apply(lambda row: row.strip())

        return df

    def connect(self, user, passwd, host, db, schema):
        """ Returns an instance of class DatabaseConnection with connection details set """
        engine = create_engine('postgresql+psycopg2://%s:%s@%s:5432/%s' % (
            user, passwd, host, db))
        con = engine.connect()
        if schema:
            con.execute("set search_path to %s" % schema)
        return DatabaseConnection(self, con)

    def generator_to_df(self, generator, columns=None):
        return pd.DataFrame([[val for val in row] for row in generator], columns=columns)

    def datenow(self):
        return datetime.datetime.now()

    def dateend(self):
        return datetime.date(year=2099, month=12, day=31)

class DatabaseConnection:

    def __init__(self, user, passwd, host, db, schema):
        """
        Initialize a database connection
        :param user: username
        :param passwd: password
        :param host: host
        :param db: database
        :param schema: schema
        """
        self._user = user
        self._passwd = passwd
        self._host = host
        self._db = db
        self._schema = schema
        self._engine = create_engine('postgresql+psycopg2://%s:%s@%s:5432/%s' % (
            user, passwd, host, db))
        self.con = self._engine.connect()  # global connection
        self.con.execute("set search_path to %s" % schema)

    def _connect_pygrametl(self):
        """Define a postgres psycopg2 connection to facilitate pygrametl processes"""
        return psycopg2.connect("""host='%s' dbname='%s' user='%s' password='%s'""" %
                                (self._host, self._db, self._user, self._passwd))

    def _connection_test(self):
        try:
            self.con.execute("SELECT 1")
        except exc.DBAPIError as e:
            print("connection test failed")
            if e.connection_invalidated:
                print("Connection invalidated")
            self.con = self._engine.connect()
            self.con.execute("set search_path to %s" % self._schema)
            print("Reconnected")

    def query(self, query):
        """
        :param query: database query
        :return: generator
        """
        self._connection_test()
        return self.con.execute(query)

    def append_df_to_table(self, df, table, dtype_dict=None):
        """
        Appends data to a database table
        :param df: pandas dataframe to write to table
        :param table: database table name
        :param dtype_dict: {column_name: SQLAlchemy_data_type} eg. from sqlalchemy.types import NVARCHAR
        :return: None
        """
        self._connection_test()
        df.to_sql(table, con=self.con, schema=self._schema, if_exists='append', index=False, dtype=dtype_dict)

    def update_table(self, sql):
        self.con.execute(sql)

    def update_scd_type_one(self, df, dimension_table, key, attributeslist, lookupatts, type1atts):
        """
        Updates SCD Type 1 Dimension Table
        :param df: dataframe of dimension
        :param dimension_table: table to update
        :param key: primary key of the table to update
        :param attributeslist: list of dimension columns
        :param lookupatts: attributes (columns) to lookup for updating
        :param type1atts: attributes (columns) to update
        :return: None
        """
        from pygrametl.tables import TypeOneSlowlyChangingDimension

        output = json.loads(df.to_json(orient='records', date_format='iso'))
        pgetlconn = pygrametl.ConnectionWrapper(connection=self._connect_pygrametl())
        pgetlconn.setasdefault()
        pgetlconn.execute('set search_path to %s' % self._schema)
        dimension = TypeOneSlowlyChangingDimension(
            name=dimension_table,
            key=key,
            attributes=attributeslist,
            lookupatts=lookupatts,
            type1atts=type1atts,
            targetconnection=pgetlconn
        )
        for row in output:
            dimension.scdensure(row)

        pgetlconn.commit()
        pgetlconn.close()

    def table_lookup(self, df, df_lookup_column_list, table, table_lookup_column_list, table_return_column_list=[],
                     right_suffix='_lookup', indicator=True, how='left'):
        """
        Looks up values from a database table
        :param df: pandas DataFrame input table
        :param df_lookup_column_list: pandas DataFrame columns to look up
        :param table: database table to look up
        :param table_lookup_column_list: database table columns that corresponds to df_lookup_column_list
        :param table_return_column_list: columns to return from the database table. Do not add column to this field if
               the field is already in table_lookup_column_list 
        :param right_suffix: suffix to add to lookup and returned columns
        :param indicator: add indicator column _merge. If this is a string, the string is the name of the column
        :param how: how to do the join
        :return: original input pandas DataFrame with lookup columns and return columns appended
        """
        self._connection_test()
        df_lookup = pd.read_sql("SELECT %s FROM %s" % (
            ', '.join(table_lookup_column_list+table_return_column_list), table), con=self.con)
        return pd.merge(left=df, right=df_lookup, how=how, left_on=df_lookup_column_list,
                        right_on=table_lookup_column_list, suffixes=('', right_suffix), indicator=indicator)

    def add_new_records(self, df, df_lookup_column_list, table, table_lookup_column_list):
        """
        Add NEW and CHANGED records to database table
        :param df: DataFrame to write records from
        :param df_lookup_column_list: pandas DataFrame columns to look up
        :param table: table to write records to
        :param table_lookup_column_list: database table columns that corresponds to df_lookup_column_list
        :return: None
        """
        self._connection_test()
        merged_table = self.table_lookup(df=df,
                                         df_lookup_column_list=df_lookup_column_list,
                                         table=table,
                                         table_lookup_column_list=table_lookup_column_list,
                                         table_return_column_list=[],
                                         right_suffix='_tbl', indicator='record_exists')
        new_records = merged_table[merged_table['record_exists'] == 'left_only']
        new_records = new_records.drop('record_exists', axis=1)
        self.append_df_to_table(df=new_records, table=table)

    def find_old_records(self, df, df_lookup_column_list, table, table_lookup_column_list, table_pk):
        """
        Add NEW and CHANGED records to database table
        :param df: DataFrame to write records from
        :param df_lookup_column_list: pandas DataFrame columns to look up
        :param table: table to write records to
        :param table_lookup_column_list: database table columns that corresponds to df_lookup_column_list
        :return: None
        """
        self._connection_test()
        merged_table = self.table_lookup(df=df,
                                         df_lookup_column_list=df_lookup_column_list,
                                         table=table,
                                         table_lookup_column_list=table_lookup_column_list,
                                         table_return_column_list=[table_pk],
                                         right_suffix='_tbl', indicator='record_exists', how='right')
        old_records = merged_table[merged_table['record_exists'] == 'right_only']
        old_records = old_records.drop('record_exists', axis=1)[[table_pk]]
        return old_records

