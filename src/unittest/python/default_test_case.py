import unittest

from sparkle_hypothesis import load_default_profile


class DefaultTestCase(unittest.TestCase):
    load_default_profile()

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        load_default_profile()
