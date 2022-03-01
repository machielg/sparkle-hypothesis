import unittest

from hypothesis import settings

settings.register_profile('foo', deadline=None)


class FooTestCase(unittest.TestCase):
    settings.load_profile('foo')

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        settings.load_profile('foo')
