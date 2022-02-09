# sparkle-hypothesis
Hypothesis for Spark Unit tests

Library for easily creating PySpark tests using Hypothesis. Create heterogenious test data with ease

Installation:
```bash
pip install sparkle-hypothesis
```

## Example
```python
from sparkle_hypothesis import SparkleHypothesisTestCase, save_dfs

class MyTestCase(SparkleHypothesisTestCase):
    st_groups = st.sampled_from(['Pro', 'Consumer'])

    st_customers = st.fixed_dictionaries(
        {'customer_id:long': st.integers(min_value=1, max_value=10),
        'customer_group:str': st.shared(st_groups, 'group')})

    st_groups = st.fixed_dictionaries(
        {'group_id:long': st.just(1),
         'group_name:str': st.shared(st_groups, 'group')
         })

    @given(st_customers, st_groups)
    @save_dfs()
    def test_answer_parsing(self, customers: dict, groups:dict):
        customers_df = self.spark.table('customers')
        groups_df = self.spark.table('groups')
```
