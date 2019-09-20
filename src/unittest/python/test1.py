from hypothesis import settings, given
from hypothesis.strategies import just

from foo_test_case import FooTestCase


class Test1(FooTestCase):

    @given(just('bar'))
    def test_bar(self, bar):
        self.assertIsNone(settings().deadline)

    def test_simple_bar(self):
        self.assertIsNone(settings().deadline)
