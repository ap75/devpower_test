from io import StringIO

import aiohttp
import pandas as pd


class PopulationDataFetcher:
    url = None

    @staticmethod
    def _parse(html_tables):
        raise NotImplementedError("Цей метод має бути реалізований у дочірньому класі")

    async def fetch(self):
        try:
            # Запит до URL
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url) as response:
                    response.raise_for_status()  # викликає виключення у разі помилки HTTP
                    html_text = await response.text()

            # Обробка HTML
            html_io = StringIO(html_text)
            tables = pd.read_html(html_io)
            df = self._parse(tables)

            # Перетворення даних
            df.columns = ["country", "region", "population"]
            df["population"] = df["population"].astype("Int64")
            return df[["country", "region", "population"]]

        except aiohttp.ClientError as e:
            # Обробка мережевих помилок
            print(f"Помилка під час HTTP запиту: {e}")
            return None
        except aiohttp.http_exceptions.HttpProcessingError as e:
            # Обробка помилок HTTP
            print(f"Сталася помилка HTTP: {e}")
            return None
        except Exception as e:
            # Обробка інших помилок
            print(f"Сталася помилка: {e}")
            return None


class WikipediaPopulationDataFetcher(PopulationDataFetcher):
    url = "https://en.wikipedia.org/w/index.php?title=List_of_countries_by_population_(United_Nations)&oldid=1215058959"

    @staticmethod
    def _parse(html_tables):
        df = html_tables[0]
        return df.iloc[1:-1, [0, 4, 2]]


class StatisticsTimesPopulationDataFetcher(PopulationDataFetcher):
    url = "https://statisticstimes.com/demographics/countries-by-population.php"

    @staticmethod
    def _parse(html_tables):
        df = html_tables[1]
        return df.iloc[:-1, [0, 8, 3]]
