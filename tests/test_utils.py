from datetime import date, datetime

from hypothesis import given, assume, settings
from hypothesis.strategies import dates, just

from tests.default_test_case import DefaultTestCase
from sparkle_hypothesis import simple_text_or_none, none_or, d_to_s, d_to_dt, today


class UtilsTestCase(DefaultTestCase):

    @given(simple_text_or_none)
    def test_simple_text_none_none(self, text):
        self.assertIsNotNone(settings().deadline)
        assume(text is None)
        self.assertIsNone(text)

    @given(simple_text_or_none)
    def test_simple_text(self, text):
        assume(text is not None)
        self.assertEqual("ABC", text)

    @given(none_or('FOO'))
    def test_simple_text_none(self, text):
        assume(text is not None)
        self.assertEqual('FOO', text)

    @given(dates(min_value=date(1999, 12, 31), max_value=date(2000, 1, 1)).map(d_to_s))
    def test_date_to_string(self, date_str):
        self.assertIsInstance(date_str, str)
        self.assertIn(date_str, ['1999-12-31', '2000-01-01'])

    @given(just(today).map(d_to_dt))
    def test_date_to_datetime(self, dt):
        self.assertIsInstance(dt, datetime)
        self.assertEqual(today, dt.date())
        self.assertGreaterEqual(dt.hour, 1)
        self.assertLessEqual(dt.hour, 23)
