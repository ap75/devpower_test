import os
import asyncio
from models import init_db, Session, Country
from parsers import fetch_data_wikipedia, fetch_data_statisticstimes


DATA_SOURCE = os.getenv('DATA_SOURCE', 'wikipedia')

async def get_data():
    """ Обирає джерело даних та завантажує їх """
    match DATA_SOURCE:
        case 'wikipedia':
            df = await fetch_data_wikipedia()
        case 'statisticstimes':
            df = await fetch_data_statisticstimes()
        case _:
            print(f'Unknown data source: {DATA_SOURCE}')
            return

    await save_data_to_db(df)

async def save_data_to_db(df):
    """ Зберігає дані до бази даних """
    session = Session()
    for _, row in df.iterrows():
        country = Country(name=row['country'], region=row['region'], population=row['population'])
        session.add(country)
    session.commit()
    session.close()

if __name__ == '__main__':
    init_db()
    asyncio.run(get_data())
