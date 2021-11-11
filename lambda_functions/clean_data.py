import pandas as pd
import boto3


NYT_URL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
JH_URL = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1597745458139r0.1522278252925191'


def lambda_handler(event, context):

    region = REGION
    bucket = BUCKETNAME
    s3 = boto3.resource('s3')

    nyt_df = pd.read_csv(NYT_URL)
    nyt_df.rename(columns={"date": "Date", "cases": "Cases",
                           "deaths": "Deaths"}, inplace=True)
    nyt_df["Date"] = pd.to_datetime(nyt_df["Date"]).dt.strftime('%Y-%m-%d')
    nyt_df_cleaned = nyt_df.drop(0).reset_index(drop=True)

    jh_df = pd.read_csv(JH_URL)
    jh_df.rename(columns={"Country/Region": "Country"}, inplace=True)
    jh_df["Date"] = pd.to_datetime(jh_df["Date"]).dt.strftime('%Y-%m-%d')
    jh_slice = jh_df.Country.str.contains('US')
    jh_df = jh_df.drop(
        columns={'Province/State', 'Country', 'Confirmed', 'Deaths'})

    jh_df['Recovered'] = jh_df['Recovered'].astype('Int64')
    jh_df_cleaned = jh_df[jh_slice]

    cleaned_data = nyt_df_cleaned.merge(jh_df_cleaned, on='Date')

    # Because JH stops tracking recoveries, provided the last non-zero value going forward
    cleaned_data['Recovered'] = (cleaned_data['Recovered'].mask(
        cleaned_data['Recovered'] == 0).ffill(downcast=None))
    cleaned_data['Date'] = cleaned_data['Date'].astype(str)
    cleaned_data = cleaned_data.fillna(0)

    cleaned_data.to_csv('/tmp/FILENAME.csv', index=False,
                        header=True, date_format='%Y-%m-%d')

    return s3.meta.client.upload_file('/tmp/FILENAME.csv', 'BUCKETNAME', 'FILENAME.csv')
