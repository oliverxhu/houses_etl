import json
import pandas as pd
import ETLTools
import time
import sqlalchemy

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
        if kwargs.get('connect_data', None):
            self.con_data = ETLTools.DatabaseConnection(
                kwargs['dt_user'], kwargs['dt_passwd'], kwargs['dt_host'], kwargs['dt_db'], kwargs['dt_schema'])

    """Generic Functions"""
    def _control(self, table):
        """
        Returns a dataframe of the staging.control table
        :param table: Table name in control table
        :return: pandas DataFrame of staging.control
        """
        return self.generator_to_df(self.con_staging.query(
            "SELECT source_col_name, dest_col_name, \"type\" FROM control WHERE dest_table_name = '%s'" % table),
            columns=['source_col_name', 'dest_col_name', 'type'])

    def _control_mapping_columns(self, table):
        """
        Returns source and destination mapping columns of table 
        :param table: relevant destination table
        :return: DICTIONARY of source and destination mapping lists
        """
        df = self._control(table)
        source_mapping = list(df[df['type'] == 'mapping']['source_col_name'])
        destination_mapping = list(df[df['type'] == 'mapping']['dest_col_name'])
        return {'source_mapping': source_mapping, 'destination_mapping': destination_mapping}

    def _control_source_columns(self, table, return_as_str=False):
        """
        Finds the source columns for any given table
        :param table: destination table
        :param return_as_str: return as a string separated by "," commas
        :return: LIST of columns in the source table
        """
        if return_as_str:
            return ', '.join(list(self._control(table).dropna()['source_col_name']))
        return list(self._control(table).dropna()['source_col_name'])

    def _control_destination_columns(self, table, key=True, meta=True, mapping=True):
        """
        Finds the destination columns for any given table. Note this DOES NOT return metadata fields.
        :param table: destination table
        :param key: boolean to include the primary key of the destination table in output list
        :param meta: boolean to include metadata columns
        :param mapping: boolean to pull the mapping columns
        :return: LIST of columns in the destination table
        """

        df = self._control(table)
        if not key:
            df = df[df['type'] != 'pk']
        if not meta:
            df = df[df['type'] != 'metadata']
        if not mapping:
            df = df[df['type'] != 'mapping']
        return list(df['dest_col_name'])

    # def _control_source_destination_mapping(self, table):
    #     """
    #     Returns a generator of of mappings '{source_col:destination_col}, {source_col:destination_col'
    #     :param table: destination table
    #     :return: Dictionary of {source:destination}
    #     """
    #     mapping = dict()
    #     for colmaps in ({item[0]: item[1]} for item in json.loads(
    #             self._control(table).dropna()[['source_col', 'destination_col']].to_json(orient='records'))):
    #         mapping.update(colmaps)
    #     return mapping

    def convert_ms_dates(self, df, table):
        date_ctrl = self._control(table)
        date_cols = list(date_ctrl[date_ctrl['type'] == 'datetime']['dest_col_name'])
        for cols in date_cols:
            df[cols] = df[cols].apply(lambda x: self.from_ms_timestamp(int(x[x.find("(")+1:x.find(")")])))
        return df

    """Table Update Functions"""
    def update_agency(self, table='agency'):
        """Updates the agency table"""
        source_agency_cols = self._control_source_columns(table, return_as_str=True)
        destination_agency_cols = self._control_destination_columns(table, key=False, meta=False, mapping=True)
        dest_map = self._control_mapping_columns(table=table)['destination_mapping']
        agency_df = self.generator_to_df(self.con_staging.query("SELECT %s FROM tm_rental_listings_hourly"
                                                                % source_agency_cols), columns=destination_agency_cols)
        agency_df = agency_df.dropna(subset=['id_agency_tm'])
        agency_df = agency_df.drop_duplicates(subset=['id_agency_tm'])
        agency_df = agency_df.assign(row_insert_date=self.datenow())

        agency_df = self.convert_ms_dates(agency_df, table)
        type1 = [x for x in destination_agency_cols if x != 'id_agency_tm']
        self.con_data.update_scd_type_one(df=agency_df, dimension_table=table, key='id_agency',
                                          attributeslist=destination_agency_cols + ['row_insert_date'],
                                          lookupatts=dest_map,
                                          type1atts=type1 + ['row_insert_date']
                                          )

    def update_rental_listings(self, table='rental_listings'):
        """Completes jobs for rental_listings"""
        source_cols = self._control_source_columns(table=table)
        destination_cols = self._control_destination_columns(table, key=False, meta=False,
                                                             mapping=True)
        mapping_cols = self._control_mapping_columns(table)

        staging_listings_df = self.generator_to_df(self.con_staging.query('SELECT %s FROM tm_rental_listings_hourly'
                                                   % ', '.join(source_cols)), columns=destination_cols)
        staging_listings_df = staging_listings_df.assign(is_current=1, valid_from=self.datenow(),
                                                         valid_to=self.dateend(), row_insert_date=self.datenow())
        staging_listings_df = self.convert_ms_dates(staging_listings_df, table)

        print('adding new listings...')

        # 1) Add new records
        self.con_data.add_new_records(df=staging_listings_df, df_lookup_column_list=['id_listing_tm'],
                                      table=table, table_lookup_column_list=['id_listing_tm'])
        print('records added: %s' % [x for x in self.con_data.query("select count(*) from rental_listings")])
        print('deactivating old listings...')
        # 2) deactivate columns that aren't active anymore (ie. in datalayer but not staging)
        old_record_list = self.con_data.find_old_records(df=staging_listings_df,
                                                         df_lookup_column_list=destination_cols,
                                                         table=table,
                                                         table_lookup_column_list=destination_cols,
                                                         table_pk='id_rental_listing')
        self.con_data.query("UPDATE rental_listings SET is_current=0, valid_to='%s' WHERE id_rental_listing IN(%s)"
                            % (self.dateend(), ', '.join(old_record_list)))
        print('after deactivation: %s' % [x for x in self.con_data.query("select count(*) from rental_listings")])
        print('updating existing listings...')

        # 3) update existing records
        update_df = self.con_data.table_lookup(df=staging_listings_df,
                                               df_lookup_column_list=destination_cols,
                                               table=table,
                                               table_lookup_column_list=destination_cols,
                                               table_return_column_list=[],
                                               wheresql="WHERE is_current = 1")
        print('joining columns: %s' % destination_cols)
        print('new rows to write as version differences: %s' % len(update_df))
        staging_listings_df.to_csv("staging_listings_df.csv", index=None)
        update_df.to_csv("update_df.csv", index=None)
        update_df = update_df[update_df['_merge'] == 'left_only']
        update_df.to_csv("update_df_left_only.csv", index=None)
        if not len(update_df) == 0:
            # update old rows
            print('updating old rows...')
            for ind, row in update_df.iterrows():
                self.con_data.query("""UPDATE %s
                                       SET is_current = 0, valid_to = '%s' 
                                       WHERE id_listing_tm = '%s'
                                       AND is_current = 1
                                       """ % (table, self.datenow(), row['id_listing_tm']))
            # write new rows
            update_df = update_df[staging_listings_df.columns]

            self.con_data.append_df_to_table(df=update_df, table=table)

    def update_agent_rental(self, table='agent_rental'):
        """Updates the agent_rental table"""
        agent_source_cols = ['"ListingId"', '"Agency_Agents_0_FullName"', '"Agency_Agents_0_MobilePhoneNumber"',
                             '"Agency_Agents_0_OfficePhoneNumber"', '"Agency_Agents_0_Photo"', '"Agency_Agents_0_UrlSlug"']
        # destination_cols = self._control_destination_columns(table, key=False, meta=False, mapping=True)
        destination_cols = ['id_rental_listing', 'fullname', 'mobile', 'office_phone', 'photo', 'url_slug']
        mapping_cols = self._control_mapping_columns(table)
        destination_key = self._control(table)[self._control(table)['type'] == 'pk']['dest_col_name']

        agent_rental_df = pd.DataFrame(columns=destination_cols)
        for i in range(2):
            pull_cols = [x.replace('0', str(i)) for x in agent_source_cols]
            df = self.generator_to_df(self.con_staging.query('SELECT %s FROM tm_rental_listings_hourly' % ', '
                                                             .join(pull_cols)), columns=destination_cols)
            df = df[(df['fullname'] != 'None')]
            df = df.dropna(subset=['fullname'])
            agent_rental_df = pd.concat([agent_rental_df, df])

        agent_rental_df = agent_rental_df.assign(row_insert_date=self.datenow())
        agent_rental_df = self.convert_ms_dates(agent_rental_df, table)
        atts = destination_cols + ['row_insert_date']
        # set agent_rental as a SCD type 1
        self.con_data.update_scd_type_one(agent_rental_df, table, key=destination_key[0],
                                          attributeslist=atts, lookupatts=['id_rental_listing'],
                                          type1atts=[x for x in atts if x not in mapping_cols['destination_mapping']])

    def update_rental_photo(self, table='rental_photo'):
        """Updates the rental_photos table"""
        # get source cols as list, and compare with long table of photo keyshe wont
        source_cols = self._control_source_columns(table)
        mapping_cols = self._control_mapping_columns(table)
        photo_cols = [x for x in source_cols if x not in ['"ListingId"', '"PictureHref"']]
        photo_df = self.generator_to_df(
            self.con_staging.query("SELECT %s FROM tm_rental_listings_hourly" % ', '.join(source_cols)),
            columns=source_cols)

        photo_df = pd.melt(photo_df, id_vars=mapping_cols['source_mapping'], value_vars=photo_cols,
                           var_name='photo_cols', value_name='photo_url')
        photo_df = photo_df.rename(columns={'"ListingId"': 'id_rental_listing'})
        photo_df = photo_df.drop('photo_cols', axis=1)
        photo_df = photo_df.dropna(subset=['photo_url'])
        photo_df = photo_df.assign(row_insert_date=self.datenow())
        photo_df = self.convert_ms_dates(photo_df, table)

        print('updating rental_photo...')
        self.con_data.add_new_records(photo_df, df_lookup_column_list=['id_rental_listing', 'photo_url'],
                                      table=table, table_lookup_column_list=['id_rental_listing', 'photo_url'])


def run_listings():
    with open('connections.json') as file:
        web_con = json.load(file)
    letl = Listings(connect_data=True, dt_user=web_con['dt_user'], dt_passwd=web_con['dt_passwd'],
                    dt_host=web_con['dt_host'], dt_db=web_con['dt_db'], dt_schema=web_con['dt_schema'],
                    connect_staging=True, st_user=web_con['st_user'], st_passwd=web_con['st_passwd'],
                    st_host=web_con['st_host'], st_db=web_con['st_db'], st_schema=web_con['st_schema'])
    letl.update_rental_listings()

run_listings()
