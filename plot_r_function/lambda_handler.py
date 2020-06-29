import boto3
import logging
import pandas as pd
import numpy as np
import io
from matplotlib import pyplot as plt
from matplotlib.dates import date2num
from matplotlib import dates as mdates
from matplotlib import ticker
from matplotlib.colors import ListedColormap
from scipy.interpolate import interp1d

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s %(name)s:%(lineno)d - %(message)s")
logger = logging.getLogger(__name__)
state_name = 'Germany'
bucket_name = 'corosim-de-r-value'

def lambda_handler(event, context):
    all_cases = read_from_aws('latest_cases.csv')
    smoothed_cases = convert_to_series(all_cases[['date','Smoothed Cases']])
    original_cases = convert_to_series(all_cases[['date','Original Cases']])
    cases_img, cases_image_name = create_cases_img(original_cases, smoothed_cases)

    rt_calculation = read_from_aws('latest_Rt.csv')
    rt_calculation['date'] =  pd.to_datetime(rt_calculation['date'], format='%Y-%m-%d')
    rt_calculation.set_index('date', inplace=True)
    rt_img, rt_image_name = create_rt_img(rt_calculation)

    upload_to_aws(cases_img, cases_image_name)
    upload_to_aws(rt_img, rt_image_name)


def read_from_aws(file_to_read):
    logger.info('Read latest_cases from S3')
    s3 = boto3.client('s3') 
    obj = s3.get_object(Bucket= bucket_name, Key= file_to_read) 
    file_from_aws = pd.read_csv(obj['Body'])
    return file_from_aws

def convert_to_series(df):
    df.set_index('date', inplace=True)
    df = df.T.squeeze()
    return df

def upload_to_aws(local_file, s3_file):
    bucket = boto3.client('s3')
    bucket.put_object(Bucket=bucket_name, Key=s3_file, Body=local_file)
    print('Put Complete')


def create_rt_img(rt_calculation):
    logger.info('Create R_t image')
    result_image_name = f'latest_Rt.png'
    fig, ax = plt.subplots(figsize=(600 / 72, 400 / 72))

    plot_rt(rt_calculation, ax, state_name, fig)
    ax.set_title(f'Real-time $R_t$ for {state_name}')
    ax.set_ylim(.5, 3.5)
    ax.xaxis.set_major_locator(mdates.WeekdayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))

    img_data = io.BytesIO()
    plt.savefig(img_data, format='png')
    img_data.seek(0)
    return img_data, result_image_name


def create_cases_img(original_cases, smoothed_cases):
    logger.info('Create cases image')
    cases_image_name = f'latest_cases.png'
    original_cases.plot(title=f"{state_name} New Cases per Day",
                        c='k',
                        linestyle=':',
                        alpha=.5,
                        label='Actual',
                        legend=True,
                        figsize=(600 / 72, 400 / 72))

    ax = smoothed_cases.plot(label='Smoothed', legend=True)
    ax.get_figure().set_facecolor('w')

    img_data = io.BytesIO()
    plt.savefig(img_data, format='png')
    img_data.seek(0)
    return img_data, cases_image_name


def plot_rt(result, ax, state_name, fig):
    ax.set_title(f"{state_name}")

    # Colors
    ABOVE = [1, 0, 0]
    MIDDLE = [1, 1, 1]
    BELOW = [0, 0, 0]
    cmap = ListedColormap(np.r_[
        np.linspace(BELOW, MIDDLE, 25),
        np.linspace(MIDDLE, ABOVE, 25)
    ])

    def color_mapped(y): return np.clip(y, .5, 1.5) - .5

    index = result['ML'].index.get_level_values('date')
    values = result['ML'].values

    # Plot dots and line
    ax.plot(index, values, c='k', zorder=1, alpha=.25)
    ax.scatter(index,
               values,
               s=40,
               lw=.5,
               c=cmap(color_mapped(values)),
               edgecolors='k', zorder=2)

    # Aesthetically, extrapolate credible interval by 1 day either side
    lowfn = interp1d(date2num(index),
                     result['Low'].values,
                     bounds_error=False,
                     fill_value='extrapolate')

    highfn = interp1d(date2num(index),
                      result['High'].values,
                      bounds_error=False,
                      fill_value='extrapolate')

    extended = pd.date_range(start=pd.Timestamp('2020-03-01'),
                             end=index[-1] + pd.Timedelta(days=1))

    ax.fill_between(extended,
                    lowfn(date2num(extended)),
                    highfn(date2num(extended)),
                    color='k',
                    alpha=.1,
                    lw=0,
                    zorder=3)

    ax.axhline(1.0, c='k', lw=1, label='$R_t=1.0$', alpha=.25)

    # Formatting
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax.xaxis.set_minor_locator(mdates.DayLocator())

    ax.yaxis.set_major_locator(ticker.MultipleLocator(1))
    ax.yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:.1f}"))
    ax.yaxis.tick_right()
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.margins(0)
    ax.grid(which='major', axis='y', c='k', alpha=.1, zorder=-2)
    ax.margins(0)
    ax.set_ylim(0.0, 3.5)
    ax.set_xlim(pd.Timestamp('2020-03-01'),
                result.index.get_level_values('date')[-1] + pd.Timedelta(days=1))
    fig.set_facecolor('w')
