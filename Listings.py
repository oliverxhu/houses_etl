import json
import pandas as pd
import datetime
import ETLTools


class Listings(ETLTools.ETLTools):
    """
    1a. Check Listing ID for new rows
    1b. Populate ALL tables with new listing information
    2a. Check active rows in website tables that aren't in staging
    2b. Set is_current to False
    3a. Check for changes to existing rows
    3b. For existing rows that changed, change the is_current, valid_from, valid_to and version
    3c. Add in the changed rows in ALL tables with new timestamps and versions
    """
    def __init__(self, **kwargs):
        """
        Initialize Listings object
        :param connect_staging: Boolean initialize object to connect to staging table
        :param st_user: staging username
        :param st_passwd: staging password
        :param st_host: staging host
        :param st_db: staging database
        :param st_schema: staging schema
        :param connect_website: Same as above, except connect to the website. Params are prefixed with 'ws'
        """
        # connection object for staging
        if kwargs.get('connect_staging', None):
            self.con_staging = ETLTools.DatabaseConnection(
                kwargs['st_user'], kwargs['st_passwd'], kwargs['st_host'], kwargs['st_db'], kwargs['st_schema'])
        # connection object for website
        if kwargs.get('connect_website', None):
            self.con_website = ETLTools.DatabaseConnection(
                kwargs['ws_user'], kwargs['ws_passwd'], kwargs['ws_host'], kwargs['ws_db'], kwargs['ws_schema'])

    """Generic Functions"""
    def control(self, table):
        """
        Returns a dataframe of the staging.control table
        :param table: Table name in control table
        :return: pandas DataFrame of staging.control
        """
        return self.generator_to_df(self.con_staging.query(
            "SELECT source_col, destination_col, col_type FROM control WHERE table_name = %s" % table))

    def control_mapping_columns(self, table):
        """
        Returns source and destination mapping columns of table 
        :param table: relevant destination table
        :return: DICTIONARY of source and destination mapping lists
        """
        df = self.control(table)
        source_mapping = list(df[df['col_type']=='mapping']['source_col'])
        destination_mapping = list(df[df['col_type']=='mapping']['destination_col'])
        return {'source_mapping': source_mapping, 'destination_mapping': destination_mapping}

    def control_source_columns(self, table, return_as_str=False):
        """
        Finds the source columns for any given table
        :param table: destination table
        :param return_as_str: return as a string separated by "," commas
        :return: LIST of columns in the source table
        """
        if return_as_str:
            return ', '.join(list(self.control(table).dropna(subset=['source_col'])['source_col']))
        return list(self.control(table).dropna(subset=['source_col'])['source_col'])

    def control_destination_columns(self, table, key=False):
        """
        Finds the destination columns for any given table. Note this DOES NOT return metadata fields.
        :param table: destination table
        :param key: boolean to include the primary key of the destination table in output list
        :return: LIST of columns in the destination table
        """
        if key:
            return list(self.control(table).dropna(subset=['destination_col'])['destination_col'])
        df = self.control(table)
        return list(df[df['col_type']!='pk']['destination_col'])

    def control_source_destination_mapping(self, table):
        """
        Returns a generator of of mappings '{source_col:destination_col}, {source_col:destination_col'
        :param table: destination table
        :return: Dictionary of {source:destination}
        """
        mapping = dict()
        for colmaps in ({item[0]: item[1]} for item in json.loads(
                self.control(table).dropna(subset=['source_col'])[['source_col', 'destination_col']].to_json(orient='records'))):
            mapping.update(colmaps)
        return mapping

    """Table Update Functions"""
    def update_agency(self):
        """Updates the Agency table as a Slowly Changing Dimension Type 1"""
        # update AGENCY table
        agency_cols = self.control_source_columns(table='agency', return_as_str=True)
        mapping_cols = self.control_mapping_columns(table='agency')
        destination_cols = self.control_destination_columns()

        agency_df = self.generator_to_df(self.con_staging.query("SELECT %s FROM staging.tm_rental_listings_hourly"
                                                                % agency_cols), columns=agency_cols)
        agency_df.columns = destination_cols
        agency_df.assign(last_updated=datetime.datetime.now())
        self.con_website.update_scd_type_one(agency_df, dimension_table='agency', key='id_agency',
                                             attributeslist=destination_cols + ['last_updated'],
                                             lookupatts=mapping_cols['destination_col'],
                                             type1atts=destination_cols.remove('id_agency_tm'))
        # update AGENCY_BRAND table
        pass

    def insert_new_rental_listings(self):
        """Writes to the table rental_listings"""
        source_cols = self.control_source_columns(table='rental_listings')
        mapping_cols = self.control_mapping_columns(table='rental_listings')
        staging_listings_df = self.generator_to_df(self.con_staging.query('SELECT %s FROM tm_rental_listings_hourly'
                                                   % ', '.join(source_cols)), columns=source_cols)
        staging_listings_df.rename(columns=self.control_source_destination_mapping(table='rental_listings'))
        # get the agency foreign key
        agency = self.con_website.table_lookup(df=staging_listings_df, df_lookup_column_list=['id_agency_tm'],
                                               table='agency', table_lookup_column_list=['id_agency_tm'],
                                               table_return_column_list=['id_agency'])
        # construct df to load
        staging_listings_df = staging_listings_df.assign(is_current=True, data_source='tm_rental_listings_hourly',
                                                         job_run_datetime=datetime.datetime.now(),
                                                         valid_from=datetime.datetime.now(), valid_to=self.dateend(),
                                                         id_agency=agency['id_agency_lookup'])

        # app new records to table
        self.con_website.add_new_records(df=staging_listings_df, df_lookup_column_list=['id_listing_tm'],
                                         table='rental_listings', table_lookup_column_list=['id_listing_tm'])

        # update columns that aren't active anymore (ie. in datalayer but not staging)
        old_record_list = self.con_website.find_old_records(df=staging_listings_df,
                                                            df_lookup_column_list=mapping_cols['destination_cols'],
                                                            table='rental_listings',
                                                            table_lookup_column_list=mapping_cols['destination_cols'],
                                                            table_pk='id_rental_listings')
        self.con_website.query("UPDATE rental_listings SET is_current=0 AND valid_to=%s WHERE id_rental_listing IN(%s)"
                               % (self.dateend(), ', '.join(["'%s'" % list_id for list_id in old_record_list])))

        # update existing records

    def querytest(self, query):
        return self.con_staging.query(query)


# with open('connections.json') as file:
#     web_con = json.load(file)
# cls = Listings(connect_staging=True, st_user=web_con['st_user'], st_passwd=web_con['st_passwd'],
#                         st_host=web_con['st_host'], st_db=web_con['st_db'], st_schema=web_con['st_schema'])


def run_listings():
    with open('connections.json') as file:
        web_con = json.load(file)
    listings = Listings(connect_website=True, ws_user=web_con['ws_user'], ws_passwd=web_con['ws_passwd'],
                        ws_host=web_con['ws_host'], ws_db=web_con['ws_db'], ws_schema=web_con['ws_schema'],
                        connect_staging=True, st_user=web_con['st_user'], st_passwd=web_con['st_passwd'],
                        st_host=web_con['st_host'], st_db=web_con['st_db'], st_schema=web_con['st_schema'])
