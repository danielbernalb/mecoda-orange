
import requests
import pandas as pd
import datetime
import numpy as np

# constant
selected_cols = [
    "Humidity",
    "Latitude",
    "Longitude",
    "PM10",
    "PM25",
    "PM25raw",
    "Temperature",
]

# Get data from API


def get_data(url, selected_cols):
    data = requests.get(url).json()['data']['result']
    df = pd.json_normalize(data)

    # list of values or single value in data response
    if 'values' in df.columns:
        df = df.explode('values')
        df['date'] = df['values'].apply(
            lambda x: datetime.datetime.utcfromtimestamp(x[0]).date())
        df['time'] = df['values'].apply(
            lambda x: datetime.datetime.utcfromtimestamp(x[0]).time())
        df['value'] = df['values'].apply(lambda x: x[1])
        df = df.drop(columns="values")
    elif 'value' in df.columns:
        df['date'] = df['value'].apply(
            lambda x: datetime.datetime.utcfromtimestamp(x[0]).date())
        df['time'] = df['value'].apply(
            lambda x: datetime.datetime.utcfromtimestamp(x[0]).time())
        df['value'] = df['value'].apply(lambda x: x[1])

    df = df.rename(columns={
        "metric.__name__": "metric_name",
        "metric.exported_job": "station",
    })

    # remove columns not used
    df = df.drop(
        columns=[col for col in df.columns if "metric." in col]).reset_index(drop=True)

    # remove rows with no station provided
    df = df[df['station'].notnull()]

    # convert df to wide table
    df_result = _wide_table(df, selected_cols)

    # set format and replace zero values in lat-lon columns
    for col in selected_cols:
        df_result[col] = df_result[col].astype(float)
    df_result['Latitude'].replace(0, np.nan, inplace=True)
    df_result['Longitude'].replace(0, np.nan, inplace=True)

    return df_result

# function to get wide table


def _wide_table(df, selected_cols):

    df_result = pd.pivot(
        df,
        index=['station', 'date', 'time'],
        columns='metric_name',
        values='value'
    ).reset_index()

    df_result = df_result[
        ['station', 'date', 'time'] + selected_cols
    ].reset_index(drop=True)

    df_result.columns.name = ""

    return df_result



def commit(self):

    query = '{job%3D"pushgateway"}'

    url = f"http://sensor.aireciudadano.com:30887/api/v1/query?query={query}"

    obs = get_data(url, selected_cols)
