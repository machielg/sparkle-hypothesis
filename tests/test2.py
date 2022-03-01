from hypothesis import settings, given
from hypothesis.strategies import just

from tests.default_test_case import DefaultTestCase


class Test2(DefaultTestCase):

    def test_simple_foo(self):
        self.assertIsNotNone(settings().deadline)

    @given(just('foo'))
    def test_foo(self, text):
        self.assertIsNotNone(settings().deadline)
