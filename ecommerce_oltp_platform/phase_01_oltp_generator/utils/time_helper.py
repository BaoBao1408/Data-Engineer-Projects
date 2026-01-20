from datetime import datetime, date, timedelta
import random

def random_datetime(start_year: int, end_year: int) -> datetime:
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + (end - start) * random.random()

def random_date_between(start_date: date, end_date: date) -> date:
    delta_days = (end_date - start_date).days
    return start_date + timedelta(days=random.randint(0, delta_days))

def random_datetime_between(start_date: date, end_date: date) -> datetime:
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    delta_seconds = int((end_dt - start_dt).total_seconds())
    return start_dt + timedelta(seconds=random.randint(0, delta_seconds))