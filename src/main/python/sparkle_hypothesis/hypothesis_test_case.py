from abc import ABC

from sparkle_test import SparkleTestCase

from sparkle_hypothesis.pyspark_profile import register_profile, load_pyspark_profile

register_profile()
load_pyspark_profile()


class SparkleHypothesisTestCase(SparkleTestCase, ABC):
    pass
