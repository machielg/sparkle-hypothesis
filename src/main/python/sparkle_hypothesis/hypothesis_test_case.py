from abc import ABC

from sparkle_test import SparkleTestCase

from sparkle_hypothesis.pyspark_profile import load_pyspark_profile


class SparkleHypothesisTestCase(SparkleTestCase, ABC):
    load_pyspark_profile()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        load_pyspark_profile()
