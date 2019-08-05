from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
        description: load dimensional data with data from s3 staging table
        parameters:
            - connection_id: connection to redshift db
            - dimensional_table: name of the dimensional table
            - create_dimensional_table_statement: sql statement to create the table
            - insert_dimensional_table_statement: sql statement to insert data from the staging
            - mode: insert or overwrite
            - dimensional_table: name of the dimensional table
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 connection_id="",
                 dimensional_table="",
                 create_dimensional_table_statement="",
                 insert_dimensional_table_statement="",
                 mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.dimensional_table = dimensional_table
        self.create_dimensional_table_statement = create_dimensional_table_statement
        self.insert_dimensional_table_statement = insert_dimensional_table_statement
        self.mode = mode

    def execute(self, context):
        self.log.info('LoadDimensionOperator fetches the redshift hook')
        self.log.info('create dimensioanal table')
        redshift = PostgresHook(postgres_conn_id=self.connection_id)
        redshift.run(self.create_dimensional_table_statement)

        self.log.info('insert data into the dimensional table')
        if self.mode == "overwrite":
            # truncate before inserting the data
            insert_query = f"TRUNCATE {self.dimensional_table}; INSERT INTO {self.dimensional_table} {self.insert_dimensional_table_statement}"

        redshift.run(insert_query)

