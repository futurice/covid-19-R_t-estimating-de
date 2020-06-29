import pandas as pd
import numpy as np
import logging
import io
import boto3
from datetime import datetime as dt
from scipy import stats as sps

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s %(name)s:%(lineno)d - %(message)s")
logger = logging.getLogger(__name__)

DAYS_USED_IN_POSTERIOR = 7

R_T_MAX = 12
r_t_range = np.linspace(0, R_T_MAX, R_T_MAX * 100 + 1)
bucket_name = 'corosim-de-r-value'
# Gamma is 1/serial interval
# https://wwwnc.cdc.gov/eid/article/26/6/20-0357_article
GAMMA = 1 / 4

state_name = 'Germany' 

def lambda_handler(event, context):    
    all_cases = read_from_aws()
    smoothed_cases = all_cases[['date','Smoothed Cases']]
    smoothed_cases.set_index('date', inplace=True)
    smoothed_cases = smoothed_cases.T.squeeze()
    rt_calculation = calculate_rt(smoothed_cases)
    rt_file, rt_filename = create_rt_csv(rt_calculation)
    upload_to_aws(rt_file, rt_filename)

def read_from_aws():
    logger.info('Read latest_cases from S3')
    file_to_read = 'latest_cases.csv'
    s3 = boto3.client('s3') 
    obj = s3.get_object(Bucket= bucket_name, Key= file_to_read) 
    all_cases = pd.read_csv(obj['Body'])
    return all_cases
    
def upload_to_aws(local_file, s3_file):
    bucket = boto3.client('s3')
    uploadByteStream = bytes(local_file.encode('UTF-8'))
    bucket.put_object(Bucket=bucket_name, Key=s3_file, Body=uploadByteStream)
    print('Put Complete')
    
def create_rt_csv(rt_calculation):
    logger.info('Create R_t csv')
    result_filename = f'latest_Rt.csv'
    return rt_calculation.reset_index().to_csv(index=False), result_filename


def calculate_rt(smoothed_cases):
    logger.info('Calculate R_t')
    posteriors = get_posteriors(smoothed_cases)
    hdis = highest_density_interval(posteriors)

    most_likely = posteriors.idxmax().rename('ML')

    rt_calculation = pd.concat([most_likely, hdis], axis=1)
    return rt_calculation

def get_posteriors(sr, min_periods=1):
    window = DAYS_USED_IN_POSTERIOR
    lam = sr[:-1].values * np.exp(GAMMA * (r_t_range[:, None] - 1))

    # Note: if you want to have a Uniform prior you can use the following line instead.
    # I chose the gamma distribution because of our prior knowledge of the likely value
    # of R_t.

    # prior0 = np.full(len(r_t_range), np.log(1/len(r_t_range)))

    prior0 = np.log(sps.gamma(a=3).pdf(r_t_range) + 1e-14)

    likelihoods = pd.DataFrame(
        # Short-hand way of concatenating the prior and likelihoods
        data=np.c_[prior0, sps.poisson.logpmf(sr[1:].values, lam)],
        index=r_t_range,
        columns=sr.index)

    # Perform a rolling sum of log likelihoods. This is the equivalent
    # of multiplying the original distributions. Exponentiate to move
    # out of log.
    posteriors = likelihoods.rolling(window,
                                     axis=1,
                                     min_periods=min_periods).sum()
    posteriors = np.exp(posteriors)

    # Normalize to 1.0
    posteriors = posteriors.div(posteriors.sum(axis=0), axis=1)

    return posteriors

def highest_density_interval(pmf, p=.95):
    # If we pass a DataFrame, just call this recursively on the columns
    if (isinstance(pmf, pd.DataFrame)):
        return pd.DataFrame([highest_density_interval(pmf[col]) for col in pmf],
                            index=pmf.columns)

    cumsum = np.cumsum(pmf.values)
    best = None
    for i, value in enumerate(cumsum):
        for j, high_value in enumerate(cumsum[i + 1:]):
            if (high_value - value > p) and (not best or j < best[1] - best[0]):
                best = (i, i + j + 1)
                break

    low = pmf.index[best[0]]
    high = pmf.index[best[1]]
    return pd.Series([low, high], index=['Low', 'High'])