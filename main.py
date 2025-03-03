import asyncio
import os
import sys

from data_manager import DataManager
from models import init_db
from parsers import StatisticsTimesPopulationDataFetcher, WikipediaPopulationDataFetcher


class DataSourceHandler:
    """Клас для обробки джерел даних"""

    DATA_SOURCE = os.getenv("DATA_SOURCE", "wikipedia")

    async def get_data(self):
        """Обирає джерело даних та завантажує їх"""
        fetcher = {
            "wikipedia": WikipediaPopulationDataFetcher(),
            "statisticstimes": StatisticsTimesPopulationDataFetcher(),
        }.get(self.DATA_SOURCE, None)

        if not fetcher:
            print(f"Невідоме джерело даних: {self.DATA_SOURCE}")
            return

        df = await fetcher.fetch()
        if df is not None:
            DataManager.save_data(df)


class DataPrinter:
    """Клас для виведення агрегованих даних"""

    @staticmethod
    def print():
        """Друкує агреговані дані за регіонами"""
        data = DataManager.get_aggregated_data()
        for row in data:
            print(
                f"Регіон: {row.region}\nЗагальна чисельність населення: {row.total_population}\n"
                f"Найбільша країна: {row.largest_country} ({row.largest_population})\n"
                f"Найменша країна: {row.smallest_country} ({row.smallest_population})\n"
            )


if __name__ == "__main__":
    init_db()

    if len(sys.argv) > 1 and sys.argv[1] == "print":
        DataPrinter.print()
    else:
        asyncio.run(DataSourceHandler().get_data())
