from typing import Dict

from hypothesis import given, settings
from hypothesis.strategies import integers, fixed_dictionaries
from pyspark.sql import DataFrame

from sparkle_hypothesis.hypothesis_test_case import SparkleHypothesisTestCase
from sparkle_hypothesis.save_data_frames import save_dfs

st_int_dict = fixed_dictionaries(
    {'foo:int': integers(min_value=1, max_value=5)}
)


class SaveDataFramesTestCase(SparkleHypothesisTestCase):

    @given(integers(min_value=1, max_value=5))
    def test_generate_ints(self, an_int):
        self.assertIsNone(settings().deadline)
        self.assertLessEqual(an_int, 5)
        self.assertGreaterEqual(an_int, 1)

    @given(st_int_dict)
    @save_dfs()
    def test_generate_int_col(self, an_int: Dict[str, int]):
        self.assertIsInstance(an_int, dict)
        t = self.spark.table('an_int')
        self.assertFalse(t.columns[0].endswith(":int"))
        self.assertEqual(1, t.count())
        int_val = t.collect()[0].foo
        self.assertLessEqual(int_val, 5)
        self.assertGreaterEqual(int_val, 1)

    @given(st_int_dict)
    @save_dfs(input_as_df=True)
    def test_generate_data_frames(self, an_int_df: DataFrame):
        self.assertIsInstance(an_int_df, DataFrame)

        self.assertEqual(1, an_int_df.count())
        int_val = an_int_df.collect()[0].foo
        self.assertLessEqual(int_val, 5)
        self.assertGreaterEqual(int_val, 1)

        # also available as spark table
        t = self.spark.table('an_int_df')
        self.assert_frame_equal_with_sort(t, an_int_df)
