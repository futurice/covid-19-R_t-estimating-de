# Original R_t code http://systrom.com/blog/the-metric-we-need-to-manage-covid-19/
import logging
import io
import boto3

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s %(name)s:%(lineno)d - %(message)s")
logger = logging.getLogger(__name__)

state_name = 'Germany' 

def lambda_handler(event, context):
    original_cases, smoothed_cases = get_prepared_cases()
    cases_file, cases_filename = create_cases_csv(original_cases, smoothed_cases)
    
    upload_to_aws(cases_file, cases_filename)
    
def upload_to_aws(local_file, s3_file):
    bucket_name = 'corosim-de-r-value'
    bucket = boto3.client('s3')
    uploadByteStream = bytes(local_file.encode('UTF-8'))
    bucket.put_object(Bucket=bucket_name, Key=s3_file, Body=uploadByteStream)
    print('Put Complete')


def create_cases_csv(original_cases, smoothed_cases):
    logger.info('Create cases csv')
    cases_filename = f'latest_cases.csv'
    cases_file = pd.concat([original_cases.rename('Original Cases'),
                               smoothed_cases.rename('Smoothed Cases')], axis=1).to_csv(index_label='date')
    return cases_file, cases_filename
    
def get_prepared_cases():
    logger.info('Prepare cases')
    germany = get_data_from_RKI()
    cases = germany['newinfections'].rename(f"{state_name} cases")

    original_cases, smoothed_cases = prepare_cases(cases)
    return original_cases, smoothed_cases


def get_data_from_RKI():
    url = 'https://covid19publicdata.blob.core.windows.net/rki/covid19-germany-federalstates.csv'
    germany = pd.read_csv(url)
    germany['date'] = germany['date'].apply(pd.to_datetime).dt.date
    germany = germany.groupby('date').sum()
    germany_file = germany.to_csv(index_label='date')
    upload_to_aws(germany_file, 'confirmed_infections_and_deaths.csv')
    return germany


def prepare_cases(cases):
    new_cases = cases

    smoothed = new_cases.rolling(7,
                                 win_type='gaussian',
                                 min_periods=1,
                                 center=True).mean(std=2).round()

    zeros = smoothed.index[smoothed.eq(0)]
    if len(zeros) == 0:
        idx_start = 0
    else:
        last_zero = zeros.max()
        idx_start = smoothed.index.get_loc(last_zero) + 1
    smoothed = smoothed.iloc[idx_start:]
    original = new_cases.loc[smoothed.index]

    return original, smoothed
