import asyncio
import os
import sys

from sqlalchemy import func, text

from models import Country, Session, init_db
from parsers import StatisticsTimesPopulationDataFetcher, WikipediaPopulationDataFetcher


class DataSourceHandler:
    """Клас для обробки джерела даних"""

    DATA_SOURCE = os.getenv("DATA_SOURCE", "wikipedia")

    async def get_data(self):
        """Обирає джерело даних та завантажує їх"""
        match self.DATA_SOURCE:
            case "wikipedia":
                fetcher = WikipediaPopulationDataFetcher()
            case "statisticstimes":
                fetcher = StatisticsTimesPopulationDataFetcher()
            case _:
                print(f"Unknown data source: {self.DATA_SOURCE}")
                return

        df = await fetcher.fetch()
        if df is not None:
            saver = DataSaver(df)
            await saver.save()


class DataSaver:
    """Клас для збереження даних в базу даних"""

    def __init__(self, df):
        self.df = df

    async def save(self):
        session = Session()
        session.execute(text("TRUNCATE TABLE countries RESTART IDENTITY CASCADE"))
        for _, row in self.df.iterrows():
            country = Country(
                name=row["country"], region=row["region"], population=row["population"]
            )
            session.add(country)
        session.commit()
        session.close()


class DataPrinter:
    """Клас для виведення агрегованих даних"""

    async def print(self):
        session = Session()

        subquery = session.query(
            Country.region,
            Country.name,
            Country.population,
            func.first_value(Country.name)
            .over(partition_by=Country.region, order_by=Country.population.desc())
            .label("largest_country"),
            func.first_value(Country.name)
            .over(partition_by=Country.region, order_by=Country.population.asc())
            .label("smallest_country"),
        ).subquery()

        query = session.query(
            subquery.c.region,
            func.sum(subquery.c.population).label("total_population"),
            func.max(subquery.c.population).label("largest_population"),
            func.min(subquery.c.population).label("smallest_population"),
            func.max(subquery.c.largest_country).label("largest_country"),
            func.max(subquery.c.smallest_country).label("smallest_country"),
        ).group_by(subquery.c.region)

        for row in query.all():
            print(
                f"Region: {row.region}\nTotal Population: {row.total_population}\n"
                f"Largest Country: {row.largest_country} ({row.largest_population})\n"
                f"Smallest Country: {row.smallest_country} ({row.smallest_population})\n"
            )

        session.close()


if __name__ == "__main__":
    init_db()

    if len(sys.argv) > 1 and sys.argv[1] == "print":
        asyncio.run(DataPrinter().print())
    else:
        asyncio.run(DataSourceHandler().get_data())
