import pandas as pd

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

    def run_listings_main(self):
        """1a."""
        # read data from staging
        self.con_staging.query("SELECT ListingId FROM tm_rental_listings_hourly")
        self.con_staging.add_new_records()

    def populate_new_tables(self, table):
        pass
