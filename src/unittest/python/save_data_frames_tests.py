from typing import Dict

from hypothesis import given
from hypothesis.strategies import integers, fixed_dictionaries, lists
from pyspark.sql import DataFrame

from sparkle_hypothesis.hypothesis_test_case import SparkleHypothesisTestCase
from sparkle_hypothesis.pyspark_profile import load_pyspark_profile
from sparkle_hypothesis.save_data_frames import save_dfs

from sparkle_session import SparkleDataFrame

st_int_dict = fixed_dictionaries(
    {'foo:int': integers(min_value=1, max_value=5)}
)

load_pyspark_profile()


class SaveDataFramesTestCase(SparkleHypothesisTestCase):

    @given(integers(min_value=1, max_value=5))
    def test_generate_ints(self, an_int):
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

    @given(st_int_dict)
    @save_dfs(input_as_sdf=True)
    def test_generate_sparkle_data_frames(self, an_int_df: SparkleDataFrame):
        self.assertIsInstance(an_int_df, SparkleDataFrame)

        self.assertEqual(1, an_int_df.count())
        int_val = an_int_df.collect()[0].foo
        self.assertLessEqual(int_val, 5)
        self.assertGreaterEqual(int_val, 1)

        # also available as spark table
        t = self.spark.table('an_int_df')
        self.assert_frame_equal_with_sort(t, an_int_df)

    @given(lists(st_int_dict, min_size=1, max_size=2))
    @save_dfs(input_as_df=True)
    def test_generate_data_frames_from_list(self, an_int_df: DataFrame):
        self.assertIsInstance(an_int_df, DataFrame)

        self.assertGreaterEqual(an_int_df.count(), 1)
        int_val = an_int_df.collect()[0].foo
        self.assertLessEqual(int_val, 5)
        self.assertGreaterEqual(int_val, 1)

        # also available as spark table
        t = self.spark.table('an_int_df')
        self.assert_frame_equal_with_sort(t, an_int_df)

    @given(lists(st_int_dict, min_size=1, max_size=2))
    @save_dfs(input_as_sdf=True)
    def test_generate_data_frames_from_list(self, an_int_df: DataFrame):
        self.assertIsInstance(an_int_df, SparkleDataFrame)

        self.assertGreaterEqual(an_int_df.count(), 1)
        int_val = an_int_df.collect()[0].foo
        self.assertLessEqual(int_val, 5)
        self.assertGreaterEqual(int_val, 1)

        # also available as spark table
        t = self.spark.table('an_int_df')
        self.assert_frame_equal_with_sort(t, an_int_df)

    @given(lists(st_int_dict, min_size=1000, max_size=1000))
    @save_dfs(input_as_sdf=True)
    def test_generate_large_data_frame(self, an_int_df: DataFrame):
        self.assertGreaterEqual(an_int_df.count(), 1000)
        self.assertGreaterEqual(an_int_df.count(), 1000)
        self.assertGreaterEqual(an_int_df.count(), 1000)
        self.assertGreaterEqual(an_int_df.count(), 1000)
        self.assertGreaterEqual(an_int_df.count(), 1000)