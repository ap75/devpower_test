import asyncio
import requests
import pandas as pd


async def fetch_data_wikipedia():
    url = 'https://en.wikipedia.org/w/index.php?title=List_of_countries_by_population_(United_Nations)&oldid=1215058959'

    response = requests.get(url)
    tables = pd.read_html(response.text)

    df = tables[0]
    df = df.iloc[1:, [0, 4, 2]]
    df.columns = ['country', 'region', 'population']

    df['population'] = df['population'].astype('Int64')

    return df[['country', 'region', 'population']]


async def fetch_data_statisticstimes():
    url = 'https://statisticstimes.com/demographics/countries-by-population.php'

    response = requests.get(url)
    tables = pd.read_html(response.text)

    df = tables[1]
    df = df.iloc[:, [0, 8, 3]]
    df.columns = ['country', 'region', 'population']

    df['population'] = df['population'].astype('Int64')

    return df[['country', 'region', 'population']]
