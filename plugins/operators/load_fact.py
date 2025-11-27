from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 query = '',
                 destination_table = None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.destination_table = destination_table

    def execute(self, context):
        self.log.info(f"Connecting to Redshift: {self.redshift_conn_id}")
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        formatted_query = f"""
            INSERT INTO {self.destination_table}
            {self.query};
            """
        
        redshift.run(formatted_query)
