from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 destination_table = None,
                 query = '',
                 mode = 'truncate-insert',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self. destination_table = destination_table
        self.query = query
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if self.mode == 'truncate-insert':
            redshift.run(f"TRUNCATE TABLE {self.destination_table};")

        formatted_query = f"""
            INSERT INTO {self.destination_table}
            {self.query};
        """

        redshift.run(formatted_query)
