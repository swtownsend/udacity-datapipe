from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

# redshift operator that copys data from teh s3 bucket to 
# the database
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    formatted_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """
# creating the paramaters for the class
    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                s3_bucket="",
                s3_key="",
                *args, **kwargs):


        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        # creating the hooks need to access aws and the database
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #deleting any old data or bad data in the database before loading 
        # the test data 
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        # get the files from the s3 bucket
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        execution_date = kwargs["ds"]
        year = execution_date.year
        month = execution_date.month
        if rendered_key == "log_data/":
            s3_path = "{}/{}/{}/{}".format(self.s3_bucket,year,month,rendered_key)
        else:
            s3_path = "{}/{}".format(self.s3_bucket,year,month,rendered_key)
        
        # create the sql script need to copy the data into the database
        #formatted_sql = StageToRedshiftOperator.copy_sql.format(
        formatted_sql = StageToRedshiftOperator.formatted_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
        )

        # run the sql
        redshift.run(formatted_sql)





