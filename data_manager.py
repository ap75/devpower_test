from sqlalchemy import func, text

from models import Country, Session


class DataManager:
    """Клас для керування даними в базі даних"""

    @staticmethod
    def save_data(df):
        """Збереження даних у базу даних"""
        session = Session()
        session.execute(text("TRUNCATE TABLE countries RESTART IDENTITY CASCADE"))
        for _, row in df.iterrows():
            session.add(
                Country(
                    name=row["country"],
                    region=row["region"],
                    population=row["population"],
                )
            )
        session.commit()
        session.close()

    @staticmethod
    def get_aggregated_data():
        """Отримання агрегованих даних за регіонами"""
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

        result = query.all()
        session.close()
        return result
