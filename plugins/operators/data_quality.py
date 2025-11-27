from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 test_cases = None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for test in self.test_cases:
            query = test['sql']
            expected_result = test['expected']

            records = redshift.get_records(query)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality test failed. Query returned no results: {query}")
            
            num_records = records[0][0]

            if num_records < 1:
                raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
            
            if num_records != expected_result:
                raise ValueError(f'Data quality check failed. Query returned {num_records} records instead of {expected_result} records.')