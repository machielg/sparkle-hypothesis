import random
from datetime import date, datetime, timedelta

import hypothesis.strategies as st

"""
Utility functions and objects for hypothesis tests
"""


def d_to_s(d: date):
    return d.strftime('%Y-%m-%d')


def d_to_dt(d: date):
    return datetime(d.year, d.month, d.day).replace(hour=random.randint(1, 23))


def none_or(val: any):
    return st.one_of(st.none(), st.just(val))


simple_text_or_none = none_or('ABC')

today = date.today()
yesterday = today - timedelta(days=1)
day_before_yesterday = yesterday - timedelta(days=1)  # day before yesterday
