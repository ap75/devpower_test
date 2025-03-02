import os
import sys
import asyncio
from models import init_db, Session, Country
from parsers import WikipediaPopulationDataFetcher, StatisticsTimesPopulationDataFetcher
from sqlalchemy import func, text


DATA_SOURCE = os.getenv('DATA_SOURCE', 'wikipedia')

async def get_data():
    """ Обирає джерело даних та завантажує їх """
    match DATA_SOURCE:
        case 'wikipedia':
            df = await WikipediaPopulationDataFetcher().fetch()
        case 'statisticstimes':
            df = await StatisticsTimesPopulationDataFetcher().fetch()
        case _:
            print(f'Unknown data source: {DATA_SOURCE}')
            return

    if df is not None:
        await save_data_to_db(df)

async def save_data_to_db(df):
    """ Зберігає дані до бази даних """
    session = Session()
    session.execute(text('TRUNCATE TABLE countries RESTART IDENTITY CASCADE'))
    for _, row in df.iterrows():
        country = Country(name=row['country'], region=row['region'], population=row['population'])
        session.add(country)
    session.commit()
    session.close()


async def print_data():
    """ Виконує SQL-запит для виведення агрегованих даних """
    session = Session()

    subquery = session.query(
        Country.region,
        Country.name,
        Country.population,
        func.first_value(Country.name).over(
            partition_by=Country.region, order_by=Country.population.desc()).label('largest_country'),
        func.first_value(Country.name).over(
            partition_by=Country.region, order_by=Country.population.asc()).label('smallest_country')
    ).subquery()

    query = session.query(
        subquery.c.region,
        func.sum(subquery.c.population).label('total_population'),
        func.max(subquery.c.population).label('largest_population'),
        func.min(subquery.c.population).label('smallest_population'),
        func.max(subquery.c.largest_country).label('largest_country'),
        func.max(subquery.c.smallest_country).label('smallest_country')
    ).group_by(subquery.c.region)

    for row in query.all():
        print(f'Region: {row.region}\nTotal Population: {row.total_population}\n'
              f'Largest Country: {row.largest_country} ({row.largest_population})\n'
              f'Smallest Country: {row.smallest_country} ({row.smallest_population})\n')

    session.close()


if __name__ == '__main__':
    init_db()

    if len(sys.argv) > 1 and sys.argv[1] == 'print':
        asyncio.run(print_data())
    else:
        asyncio.run(get_data())