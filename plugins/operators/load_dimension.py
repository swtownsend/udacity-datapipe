from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#  operator that copys data from the staging tables  to 
# the star schema database
class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    #template_fields = ("s3_key",)


    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 conn_id="",
                 aws_credentials_id="",
                 table="",
                 sql_statement="",
                 append_data = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id 
        self.table = table
        #self.append_data = append_data
        self.sql_statement = sql_statement
        
        

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        conn_hook = PostgresHook(self.conn_id)
        execution_date = kwargs["execution_date"]
        previous_date = kwargs["prev_ds"]
        
             
        #Copying data from staging table to dimension table
        if execution_date < previous_date:
            sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.sql_statement)
            conn_hook.run(sql_statement)
        else:
            sql_statement = 'DELETE FROM %s' % self.table_name
            conn_hook.run(sql_statement)

            sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.sql_statement)
            conn_hook.run(sql_statement)

