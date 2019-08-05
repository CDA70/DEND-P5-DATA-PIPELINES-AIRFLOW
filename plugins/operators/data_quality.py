from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 connection_id="",
                 table_name="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.connection_id=connection_id
        self.table_name=table_name

    def execute(self, context):
        self.log.info('DataQualityOperator raises an error if a table is empty')
        redshift = PostgresHook(postgres_conn_id=self.connection_id)
        number_of_records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table_name}")
        if len(number_of_records[0]) < 1:
            raise ValueError(f"Table {self.table_name} is empty, data quality does not allow an empty songplay table!")
            
        self.log.info(f"Table {self.table_name} contains {number_of_records} of records. Congrats, check passed!")