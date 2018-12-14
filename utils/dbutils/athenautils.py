"""Class to abstract AWS athena access

The only way to use this class as of now is to use
with.. construct.

Use the execute_query to run SELECTs 

Requires the following environment variable to be set, 
or be present via aws config
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
AWS_ATHENA_S3_STAGING_DIR
"""

import contextlib
import boto3
from pyathenajdbc import connect
from pyathenajdbc.util import as_pandas
from pyathena import connect as connect_async
from pyathena.async_cursor import AsyncCursor
from concurrent.futures import wait
import os
import pandas as pd
import io
import time

class AthenaUtils:
    """Class for Athena Access"""

    __query_id = None

    def __init__(self):
        # Verify that all environment variable are set
        if 'AWS_ATHENA_S3_STAGING_DIR' not in os.environ:
            raise Exception("Environment variable AWS_ATHENA_S3_STAGING_DIR not set. Cannot continue")

    # Enter and Exit are not required here, but kept anyways
    # For making the interface consistent with other DB classes
    def __enter__(self): return self

    def __exit__(self, exc_type, exc_value, traceback): pass

    def execute_query_async(self, query):
        """Run a SELECT  async statement.

                Args:
                    query: The SELECT statement to be executed

                Returns:
                    Returns the result as pandas dataframe
        """
        client = boto3.client('athena')
        futures = []
        try:
            flog = open('testlog.log','w')
            flog.write('Enter method: %s \n' % str(time.ctime()))
            flog.close()
            with contextlib.closing(connect_async(cursor_class=AsyncCursor)) as conn:
                with conn.cursor(max_workers=25) as cursor:
                    query_id, future = cursor.execute(query)
                    futures.append(future)
                    # wait till athena query is executed completely
                    wait(futures)
            flog = open('testlog.log','w')
            flog.write('Query execution completed: %s \n' %str(time.ctime()))
            flog.close()

            # Getting details of given query_id
            response = client.batch_get_query_execution(QueryExecutionIds=[query_id])
            flog = open('testlog.log','w')
            flog.write('Fetched result from athena: %s \n' % str(time.ctime()))
            flog.close()
            
            status = response['QueryExecutions'][0]['Status']['State']
    
            if status != 'SUCCEEDED':
                error = response['QueryExecutions'][0]['Status']['StateChangeReason']
                raise Exception (error)

            s3_client = boto3.client('s3')
            bucket_name = os.environ["AWS_ATHENA_S3_STAGING_DIR"]
            # Reading athena result from S3
            obj = s3_client.get_object(Bucket=bucket_name[5:], Key=query_id+".csv")
            df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')

            flog = open('testlog.log','w')
            flog.write('Fetched result from S3 as dataframe: %s \n' % str(time.ctime()))
            flog.close()
            return df
        except Exception as ex:
            raise ex

    def execute_query(self, query, data=None):
        """Run a SELECT statement.

        Args:
            query: The SELECT statement to be executed
            data[Optional]: The data to be used for parametrized query

        Returns:
            Returns the result as pandas dataframe
        """
        try:
            with contextlib.closing( connect())  as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, data)
                    result_file = cursor.query_id + '.csv'

                    self.__query_id = cursor.query_id
                    
            bucket_name = os.environ["AWS_ATHENA_S3_STAGING_DIR"]
            s3_client = boto3.client('s3')
            # Remove the s3:// part from bucket name
            obj = s3_client.get_object(Bucket=bucket_name[5:], Key=result_file)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
        
        except Exception as ex:
            raise ex
        # Query executed, now load the result csv to pandas df
        return df

    def execute_non_query(self, query, data=None):
        """Run a insert/select/ddl queries statement.

        Args:
            query: The query statement to be executed
            data[Optional]: The data to be used for parametrized query

        Returns:
            Returns the result as pandas dataframe
        """
        try:
            with contextlib.closing( connect()) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, data)
        
        except Exception as ex:
            raise ex
        # Query executed, now load the result csv to pandas df

    @staticmethod
    def check_table( schema_name, table_name):
        """Check if a table exists in the given athena schema"""
        try:
            query = "SHOW TABLES FROM {0}".format(schema_name)
            with contextlib.closing( connect()) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall()
            result = table_name in [table_name[0] for table_name in result]
            return result
        except Exception as ex:
            raise ex

            
    @property
    def query_id(self): return self.__query_id
