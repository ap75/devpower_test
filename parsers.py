import aiohttp
import pandas as pd
from io import StringIO


class PopulationDataFetcher:
    url = None

    @staticmethod
    def _parse(html_tables):
        raise NotImplementedError("Цей метод має бути реалізований у дочірньому класі")

    async def fetch(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                html_text = await response.text()
                html_io = StringIO(html_text)
                df = self._parse(pd.read_html(html_io))
                df.columns = ['country', 'region', 'population']
                df['population'] = df['population'].astype('Int64')
                return df[['country', 'region', 'population']]


class WikipediaPopulationDataFetcher(PopulationDataFetcher):
    url = 'https://en.wikipedia.org/w/index.php?title=List_of_countries_by_population_(United_Nations)&oldid=1215058959'

    @staticmethod
    def _parse(html_tables):
        df = html_tables[0]
        return df.iloc[1:, [0, 4, 2]]


class StatisticsTimesPopulationDataFetcher(PopulationDataFetcher):
    url = 'https://statisticstimes.com/demographics/countries-by-population.php'

    @staticmethod
    def _parse(html_tables):
        df = html_tables[1]
        return df.iloc[:-1, [0, 8, 3]]
