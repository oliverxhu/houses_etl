import pandas as pd
from pandas.io.json import json_normalize
import datetime
import requests
import json
import ETLTools


class LocationMetadata(ETLTools.ETLTools):
    def __init__(self, **kwargs):
        # connection object for staging
        if kwargs.get('connect_staging', None):
            self.con_staging = ETLTools.DatabaseConnection(
                kwargs['st_user'], kwargs['st_passwd'], kwargs['st_host'], kwargs['st_db'], kwargs['st_schema'])
        # connection object for website
        if kwargs.get('connect_website', None):
            self.con_website = ETLTools.DatabaseConnection(
                kwargs['ws_user'], kwargs['ws_passwd'], kwargs['ws_host'], kwargs['ws_db'], kwargs['ws_schema'])
        # run location api
        if kwargs.get('location_api', None):
            data = requests.get(kwargs['location_api'])
            area = json.loads(data.text)
            self.districts = json_normalize(area, 'Districts', ['LocalityId', 'Name'], meta_prefix='LocalityMetadata')
            self.region_df = self.run_region()
            self.district_df = self.run_district()
            self.suburb_df = self.run_suburb()
            self.location_df = None
            self.adjacent_suburbs_df = None
            self._region_updated = False
            self._district_updated = False
            self._suburb_updated = False
            self._location_updated = False
            self._adjacent_suburbs_updated = False

    def location(self):
        """
        :return: normalised dataframe of all location trademe-ids and their names
        """
        suburbs_list = pd.DataFrame(columns=['SuburbId', 'Name', 'DistrictId'])
        for index, row in self.districts.iterrows():
            suburbs = json_normalize(json.loads(json.dumps(row['Suburbs'])), meta=['SuburbId', 'Name'])
            suburbs = suburbs.assign(DistrictId=row['DistrictId'])
            suburbs = suburbs.drop('AdjacentSuburbs', axis=1)
            suburbs_list = pd.concat([suburbs_list, suburbs])

        locations = pd.merge(left=self.districts.drop('Suburbs', axis=1), right=suburbs_list,
                             how='inner', left_on='DistrictId', right_on='DistrictId',
                             suffixes=('_district', '_suburb'))
        locations.columns = ['id_district_tm', 'district_name', 'id_region_tm',
                             'region_name', 'suburb_name', 'id_suburb_tm']
        locations['id_suburb_tm'] = locations['id_suburb_tm'].apply(int)  # suburbs is decimal for some reason
        return self.trim_dataframe(locations, ['district_name', 'region_name', 'suburb_name'])

    def run_region(self):
        """Populates self.region_df for `region`. Runs in the constructor."""
        region_df = self.location()[['id_region_tm', 'region_name']]
        region_df['last_updated'] = datetime.datetime.now()
        region_df = region_df.rename(columns={'region_name': 'name'})
        return region_df.drop_duplicates(subset=['id_region_tm', 'name'])

    def run_district(self):
        """Populates self.district_df for `district`. Runs in the constructor."""
        district_df = self.location()[['id_district_tm', 'district_name']]
        district_df['last_updated'] = datetime.datetime.now()
        district_df = district_df.rename(columns={'district_name': 'name'})
        return district_df.drop_duplicates(subset=['id_district_tm', 'name'])

    def run_suburb(self):
        """Populates self.suburb_df for `suburb`. Runs in the constructor."""
        suburb_df = self.location()[['id_suburb_tm', 'suburb_name']]
        suburb_df['last_updated'] = datetime.datetime.now()
        suburb_df = suburb_df.rename(columns={'suburb_name': 'name'})
        return suburb_df.drop_duplicates(subset=['id_suburb_tm', 'name'])

    def run_location(self):
        """Populates self.location_df for database table `location`"""
        if not (self._region_updated and self._district_updated and self._suburb_updated):
            print("Warning: Running location table without having updated district, region and suburb"
                  " in this class instance")
        location_df = self.location()[['id_district_tm', 'id_region_tm', 'id_suburb_tm']]
        for tbltype in ('region', 'district', 'suburb'):
            location_df = self.con_website.table_lookup(df=location_df, df_lookup_column_list=['id_%s_tm' % tbltype],
                                                        table='analytics_%s' % tbltype,
                                                        table_lookup_column_list=['id_%s_tm' % tbltype],
                                                        table_return_column_list=['id_%s' % tbltype],
                                                        right_suffix='_tblid', indicator=False)
        location_df = location_df.drop(['id_district_tm', 'id_region_tm', 'id_suburb_tm'], axis=1)
        location_df['last_updated'] = datetime.datetime.now()
        self.location_df = location_df.rename(columns={
            'id_region_tblid': 'id_region',
            'id_district_tblid': 'id_district',
            'id_suburb_tblid': 'id_suburb'
        })

    def run_adjacent_suburbs(self):
        """Populates self.adjacent_suburbs_df for database table `adjacent_suburbs`"""
        suburbs_list = pd.DataFrame(columns=['AdjacentSuburbId', 'SuburbId'])
        for index, row in self.districts.iterrows():
            suburbs = json_normalize(json.loads(json.dumps(row['Suburbs'])), 'AdjacentSuburbs', ['SuburbId', 'Name'])
            suburbs.columns = ['AdjacentSuburbId', 'SuburbId', 'Name']
            suburbs = suburbs.drop('Name', axis=1)
            suburbs_list = pd.concat([suburbs_list, suburbs])

        # get rid of rows where suburb == adjacent suburb (the api lists the own suburb as an adjacent suburb)
        suburbs_list['check'] = suburbs_list['AdjacentSuburbId'] - suburbs_list['SuburbId']
        suburbs_list = suburbs_list[suburbs_list['check'] != 0]
        suburbs_list = suburbs_list.drop('check', axis=1)
        suburbs_list.columns = ['id_adjacent_suburb_tm', 'id_suburb_tm']
        suburbs_list = self.con_website.table_lookup(df=suburbs_list, df_lookup_column_list=['id_suburb_tm'],
                                                     table='analytics_suburb',
                                                     table_lookup_column_list=['id_suburb_tm'],
                                                     table_return_column_list=['id_suburb'], right_suffix='_tblid',
                                                     indicator=False)
        suburbs_list = self.con_website.table_lookup(df=suburbs_list, df_lookup_column_list=['id_adjacent_suburb_tm'],
                                                     table='analytics_suburb',
                                                     table_lookup_column_list=['id_suburb_tm'],
                                                     table_return_column_list=['id_suburb'], right_suffix='_adjtblid',
                                                     indicator=False)
        suburbs_list = suburbs_list.drop(['id_adjacent_suburb_tm', 'id_suburb_tm', 'id_suburb_tm_adjtblid'], axis=1)
        suburbs_list['last_updated'] = datetime.datetime.now()
        suburbs_list = suburbs_list.dropna(axis=0)
        self.adjacent_suburbs_df = suburbs_list.rename(columns={
            'id_suburb_tblid': 'id_suburb',
            'id_suburb_adjtblid': 'id_suburb_adjacent'
        })

    def update_region_table(self):
        """Update region metadata table"""
        self.con_website.update_scd_type_one(self.region_df, dimension_table='analytics_region', key='id_region',
                                             attributeslist=['id_region_tm', 'name', 'last_updated'],
                                             lookupatts=['id_region_tm'], type1atts=['name', 'last_updated'])
        self._region_updated = True

    def update_district_table(self):
        """Update district metadata table"""
        self.con_website.update_scd_type_one(self.district_df, dimension_table='analytics_district', key='id_district',
                                             attributeslist=['id_district_tm', 'name', 'last_updated'],
                                             lookupatts=['id_district_tm'], type1atts=['name', 'last_updated'])
        self._district_updated = True

    def update_suburb_table(self):
        """Update suburb metadata table"""
        self.con_website.update_scd_type_one(self.suburb_df, dimension_table='analytics_suburb', key='id_suburb',
                                             attributeslist=['id_suburb_tm', 'name', 'last_updated'],
                                             lookupatts=['id_suburb_tm'], type1atts=['name', 'last_updated'])
        self._suburb_updated = True

    def write_location_table(self):
        """Only for first time writing, or after truncating"""
        self.run_location()
        self.con_website.append_df_to_table(df=self.location_df, table='analytics_location')

    def update_location_table(self):
        """Update location metadata table. This requires all 3 location tables to be updated"""
        self.run_location()
        self.con_website.add_new_records(df=self.location_df,
                                         df_lookup_column_list=['id_region', 'id_district', 'id_suburb'],
                                         table='analytics_location',
                                         table_lookup_column_list=['id_region', 'id_district', 'id_suburb'])
        self._location_updated = True

    def update_adjacent_suburbs_table(self):
        """Update adjacent suburbs metadata table. This requires suburb metadata table to be updated"""
        self.run_adjacent_suburbs()
        self.con_website.add_new_records(df=self.adjacent_suburbs_df,
                                         df_lookup_column_list=['id_suburb', 'id_suburb_adjacent'],
                                         table='analytics_adjacent_suburbs',
                                         table_lookup_column_list=['id_suburb', 'id_suburb_adjacent'])
        self._adjacent_suburbs_updated = True


def run_metadata():
    with open('API.json') as file:
        location_api = json.load(file)
        location_api = location_api['location_api']

    with open('connections.json') as file:
        web_con = json.load(file)

    location_md = LocationMetadata(location_api=location_api,
                                   connect_website=True, ws_user=web_con['ws_user'], ws_passwd=web_con['ws_passwd'],
                                   ws_host=web_con['ws_host'], ws_db=web_con['ws_db'], ws_schema=web_con['ws_schema'])
    location_md.update_district_table()
    location_md.update_region_table()
    location_md.update_suburb_table()
    location_md.update_location_table()
    location_md.update_adjacent_suburbs_table()
