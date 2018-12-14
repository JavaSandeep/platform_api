"""Hive Utils - Running on AWS EMR"""
from pyhive import hive
import contextlib
import boto3

class HiveUtils:
    """Class to run query on Hive"""
    def __init__(self, config, host_name = None):
        # TODO: Add logging and error handling
        self.__host = config.get('host', None) if host_name is None else host_name
        self.__username = config.get('username', None)
        self.__port = config.get('port', None)
        self.__workspace = config.get('s3_workspace_bucket', None)
        self.__output = config.get('s3_output_bucket', None)
        self.__timeout = 10000

    # Enter and Exit are not required here, but kept anyways
    # For making the interface consistent with other DB classes
    def __enter__(self): return self

    def __exit__(self, exc_type, exc_value, traceback): pass

    def execute_query(self, query, data=None):
        """Run a SELECT statement.

        Args:
            query: The SELECT statement to be executed
            data[Optional]: The data to be used for parametrized query

        Returns:
            Returns the result as pandas dataframe
        """
        try:
            with contextlib.closing( hive.connect(
                    host = self.__host,
                    username = self.__username
                    )) as conn:
                with contextlib.closing(conn.cursor()) as cursor:
                    cursor.execute(query, data)
                    result = True
                    # In case of dml this is -1
                    if cursor.rowcount != -1:
                        columns = cursor.description
                        result = \
                            [{columns[index][0]:column for
                              index, column in enumerate(value)}
                             for value in cursor.fetchall()]
                 
                    
#            bucket_name = os.environ["AWS_ATHENA_S3_STAGING_DIR"]
#            s3_client = boto3.client('s3')
#            # Remove the s3:// part from bucket name
#            obj = s3_client.get_object(Bucket=bucket_name[5:], Key=result_file)
#            df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
        
        except Exception as ex:
            raise (ex)
        return result


