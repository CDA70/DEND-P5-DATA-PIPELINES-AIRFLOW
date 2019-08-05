from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
        description: load s3 data to redshift db staging tables
        parameters:
            - connection_id: connection to redshift db
            - aws_credentials_id: Amazon Web Services connection credential
            - staging_table: name of the staging table
            - s3_bucket: name of the s3 bucket
            - s3_path: name of the s3 path
            - delimiter: field delimiter
            - ignore_headers: 0 or 1
            - file_format: csv or json

    """
    ui_color = '#358140'
    template_fields = ("s3_key",)
    sql_template_csv = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            IGNOREHEADER {}
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            REGION 'us-west-2'
            DELIMITER '{}'
        """
    
    sql_template_json = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION 'us-west-2'
            FORMAT AS JSON '{}'
        """

    @apply_defaults
    def __init__(self,
                 connection_id="",
                 aws_credentials_id="",
                 staging_table="",
                 s3_bucket="",
                 delimiter="",
                 ignore_headers=1,
                 file_format="",
                 s3_key="",
                 sql="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.connection_id=connection_id
        self.aws_credentials_id=aws_credentials_id
        self.staging_table=staging_table
        self.s3_bucket=s3_bucket
        self.delimiter=delimiter
        self.ignore_headers=ignore_headers
        self.file_format=file_format
        self.s3_key=s3_key
        self.sql=sql

    def execute(self, context):
        self.log.info('start StageToRedshiftOperator')
        self.log.info(f"aws_credentials: {self.aws_credentials_id}")
        aws_hook = AwsHook(self.aws_credentials_id)
        self.log.info(f"aws hook: {aws_hook}")
        credentials = aws_hook.get_credentials()
        self.log.info(f"credentials: {credentials}")
        redshift = PostgresHook(postgres_conn_id=self.connection_id)
        self.log.info(f"redshift: {redshift}")
        self.log.info(f"redshift connection id: {self.connection_id}")

        self.log.info(f"Create staging table: {self.staging_table}")
        redshift.run(format(self.sql))
        
        self.log.info("Truncate the data in the staging table")
        redshift.run("truncate {}".format(self.staging_table))

        self.log.info("start the copy process")
        # set s3 path
        rendered_key = self.s3_key.format(**context)
        self.log.info("rendered key: {}".format(rendered_key))
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"s3 path:  {s3_path}")

        if self.file_format == "csv":
            self.log.info("file format is CSV")
            sql_template = StageToRedshiftOperator.sql_template_csv.format(
                self.staging_table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter    
            )
        elif self.file_format == "json":
            self.log.info("file format is JSON!!!")
            if self.staging_table == "staging_events":
                format_as_json = 's3://udacity-dend/log_json_path.json'
            else:
                format_as_json = 'auto'
            self.log.info(f"format as json is: {format_as_json}")    
            sql_template = StageToRedshiftOperator.sql_template_json.format(
                self.staging_table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                format_as_json
            )
        self.log.info(f"sql template: {sql_template}")
        redshift.run(sql_template)

