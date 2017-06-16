# test
# testnodbwrite
mode = 'testnodbwrite1'

import boto3
import datetime
from bs4 import BeautifulSoup
import urllib
import pandas as pd
from sqlalchemy import create_engine

job_run_datetime = datetime.datetime.now().replace(microsecond=0)

print('nz_mortgage_rate_scraping.py')
print('run date time: ' + str(job_run_datetime))

url = 'http://www.mortgagerates.co.nz/'

soup = BeautifulSoup(urllib.request.urlopen(url).read(), 'lxml')

div = soup.find('div', {'class': 'mortgageRatesTable'})
table = div.find('table')
rows = table.findAll('tr')
data = []
for tr in rows:
    cols = tr.findAll('td')
    col_data = []
    for ele in cols:
        if len(ele.text.strip()) != 0:
            col_data.append(ele.text.strip())
        else:
            col_data.append(ele.find('a')['href'])
    data.append([ele for ele in col_data if ele])

col_names = []
for i in range(0, len(cols)):
    col_names.append(cols[i]['class'][0])

data = data[1:len(data) - 2]

df_final = pd.DataFrame(data)
df_final.columns = col_names
df_final = df_final.dropna(thresh=6)

rate_cols = [col for col in df_final.columns if 'rate' in col]
df_final = df_final.replace({'-': None}).replace({'[^\x00-\x7F]': ''}, regex=True)
df_final = df_final.apply(pd.to_numeric, errors='ignore')
df_final['job_run_datetime'] = job_run_datetime
df_final['countryISO'] = 'NZ'

# print(df_final)

# write listings out to csv file
file_path = '/home/ubuntu/projects/housing/data/mortgage_rates/nz_mortgage_rates_hourly_' + str(
    job_run_datetime).replace(':', '_') + '.csv'
df_final.to_csv(path_or_buf=file_path)

### upload files to aws s3 bucket

bucket_name = 'housingdatawebsite'

# connect to aws s3
print('connecting to aws s3')
s3 = boto3.resource('s3')

# upload
print('uploading csv to housingdatawebsite bucket')
s3.meta.client.upload_file(file_path, bucket_name,
                           'data/mortgage_rates/nz_mortgage_rates_hourly_' + str(job_run_datetime).replace(':',
                                                                                                           '_') + '.csv')

## write listings to db ###

if mode == 'testnodbwrite':
    print('mode = testnodbwrite')
else:

    # create engine to connect to postgressql
    driver = 'postgresql'
    username = 'housingdata'
    host = 'housing-postgres.ct0tluqftf3s.ap-southeast-2.rds.amazonaws.com'
    password = 'housingdata123'
    port = '5432'
    database = 'housing'
    schema = 'staging'
    table_name = 'mortgage_rates_hourly'

    engine = create_engine(driver + '://' + username + ':' + password + '@' + host + ':' + port + '/' + database)

    # write to postgresql database (append)
    df_final.to_sql(table_name, engine, if_exists='append', index=False, schema=schema)

run_duration = datetime.datetime.now() - job_run_datetime
print(run_duration)