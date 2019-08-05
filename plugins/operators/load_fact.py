from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
        description: load fact data with data from s3 staging table
        parameters:
            - connection_id: connection to redshift db
            - fact_table: name of the fact table
            - create_fact_table_statement: sql statement to create the table
            - insert_fact_table_statement: sql statement to insert data from the staging
            - mode: insert or overwrite
            - fact_table: name of the fact table
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 connection_id="",
                 fact_table="",
                 create_fact_table_statement="",
                 insert_fact_table_statement="",
                 mode="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.fact_table = fact_table
        self.create_fact_table_statement = create_fact_table_statement
        self.insert_fact_table_statement = insert_fact_table_statement
        self.mode = mode
    

    def execute(self, context):
        self.log.info('LoadFactOperator fetches the redshift hook')
        self.log.info('create fact table')
        redshift = PostgresHook(postgres_conn_id=self.connection_id)
        redshift.run(self.create_fact_table_statement)

        self.log.info('insert data into the fact table')
        if self.mode == "overwrite":
            # truncate before inserting the data
            insert_query = f"TRUNCATE {self.fact_table}; INSERT INTO {self.fact_table} {self.insert_fact_table_statement}"

        redshift.run(insert_query)
