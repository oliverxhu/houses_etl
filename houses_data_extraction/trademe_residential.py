# test
# testnodbwrite
mode = 'testnodbwrite1'

### import packages ###

import requests
import math
import pandas as pd
import json
import re
import datetime
import boto3
import psycopg2
from sqlalchemy import create_engine


### define functions ###

def flatten_json(y):
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


def convert_regex(text):
    return re.sub(r"(?<=\w(?=(\")\w))", '`~', text).replace('`~"', '\'')


### run date time ###

job_run_datetime = datetime.datetime.now().replace(microsecond=0)
print('trademe_residential_hourly.py')
print('run date time: ' + str(job_run_datetime))

### determine number of iterations required to extract all rental listings from trademe api ###

# base url
baseurl = 'https://api.trademe.co.nz/v1/Search/Property/Residential.json?photo_size=FullSize&page='
# set pagesize to maximum allowable
if mode != 'test' and mode != 'testnodbwrite':
    pagesize = 500
else:
    pagesize = 5

# authorisation header
headers = {
    'Authorization': 'OAuth oauth_consumer_key="2AFE8295CDC58D681F6DEE0039F2C88CFC ",oauth_token="94C430759E328769D19F4376BCE8436EB8",oauth_signature_method="PLAINTEXT",oauth_signature="9DDA657BE96819372F0C07DF63AEAB4502&04F1FF22E5230CAAECA2C350B7880CBAFD"'}
# first page url
initial_url = baseurl + str(1) + '&rows=' + str(pagesize)
# make request
r = requests.get(initial_url, headers=headers)

if r.status_code == 200:

    # get json object from request
    json_data = r.json()

    # determine iterations needed and store first page data
    records = json_data['TotalCount']
    print('records: ' + str(records))
    if mode != 'test' and mode != 'testnodbwrite':
        iterations = math.ceil(records / pagesize)
    else:
        iterations = 2
    print('iterations required: ' + str(iterations))

    # extract listing data from overall json
    json_listings = json_data['List']

    # flatten data and store in dataframe
    test = flatten_json(json_listings)
    test = json.dumps(test)
    test = (("[" + re.sub(r"\"[0-9]+_", "\"", test).replace('"ListingId":', '},{"ListingId":') + ']').replace('[{},',
                                                                                                              '[')).replace(
        ', }', '}')
    test = json.loads(test)
    df = pd.DataFrame(test)
    # create final dataframe to store all data for run
    df_final = df

    # selecting columns of interest
    # adjsub_cols = [col for col in df.columns if 'AdjacentSuburb' not in col and 'Agency_Agents' not in col]
    # print(list(df.columns))
    # print(adjsub_cols)
    # print(df[adjsub_cols])


    ### get the rest of the data ###
    if iterations > 1:

        # call trademe api i-1 times
        for i in range(2, iterations + 1):
            url = baseurl + str(i) + '&rows=' + str(pagesize)
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                print(i)
                json_data = r.json()
                json_listings = json_data['List']
                # flatten data and store in dataframe
                test = flatten_json(json_listings)
                test = json.dumps(test)
                test = (
                    ("[" + re.sub(r"\"[0-9]+_", "\"", test).replace('"ListingId":', '},{"ListingId":') + ']').replace(
                        '[{},',
                        '[')).replace(
                    ', }', '}')
                test = json.loads(test)
                df = pd.DataFrame(test)
                # append flattened records to final dataframe
                df_final = df_final.append(df, ignore_index=True)

            else:
                print('Phase 2 ERROR status code: ' + r.status_code)


                # df_final.to_csv(path_or_buf='C:\\XY\\1\\Projects\\TrademeDataExtraction\\test.csv')
    # append
    df_final['job_run_datetime'] = job_run_datetime
    # drop duplicates
    df_final = df_final.drop_duplicates()
    # print(df_final)
else:
    print('Phase 1 ERROR status code: ' + r.status_code)

# # write listings out to csv file
# file_path = '/home/ubuntu/projects/housing/data/residential/tm_residential_hourly_' + str(job_run_datetime).replace(':',
#                                                                                                                     '_') + '.csv'
# df_final.to_csv(path_or_buf=file_path)
#
# ### upload files to aws s3 bucket
#
# bucket_name = 'housingdatawebsite'
#
# # connect to aws s3
# print('connecting to aws s3')
# s3 = boto3.resource('s3')
#
# # upload
# print('uploading csv to housingdatawebsite bucket')
# s3.meta.client.upload_file(file_path, bucket_name,
#                            'data/residential/tm_residential_hourly_' + str(job_run_datetime).replace(':', '_') + '.csv')

if mode == 'testnodbwrite':
    print('mode = testnodbwrite')
else:
    ### write listings to db ###

    # create engine to connect to postgressql
    driver = 'postgresql'
    username = 'housingdata'
    password = 'housingdata123'
    host = 'housing-postgres.ct0tluqftf3s.ap-southeast-2.rds.amazonaws.com'
    port = '5432'
    database = 'housing'
    schema = 'staging'
    table_name = 'tm_residential_listings_hourly'

    # truncate table
    conn = psycopg2.connect(host=host, user=username, password=password, port=port, database=database)
    print('creating session to database')
    cur = conn.cursor()
    print('truncating table')
    cur.execute("Truncate %s.%s" % (schema, table_name))
    print('committing script')
    conn.commit()
    print('closing connection')
    conn.close()

    # create engine for sqlalchemy
    engine = create_engine(driver + '://' + username + ':' + password + '@' + host + ':' + port + '/' + database)

    # write to postgresql database (append)
    df_final.to_sql(table_name, engine, if_exists='append', index=False, schema=schema)

    # # dimensionalise (sort of...) the newly written data
    # conn = psycopg2.connect(host=host, user=username, password=password, port=port, database=database)
    # print('creating session to database for dimensionalisation')
    # cur = conn.cursor()
    # print('executing staging.rental_extract_id()')
    # cur.execute("select * from staging.residential_extract_id()")
    # print('executing staging.rental_extract_detail()')
    # cur.execute("select * from staging.residential_extract_detail()")
    # print('committing script')
    # conn.commit()
    # print('closing connection')
    # conn.close()

run_duration = datetime.datetime.now() - job_run_datetime
print(run_duration)