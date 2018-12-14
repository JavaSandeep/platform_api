"""Class to abstract database access

This class should be used for all database access.
The only way to use this class as of now is to use
with.. construct.

Use the execute_query to run SELECTs and execute_non_query
to run DMLs.

Example Configuration:
    {
        "db_host" : "database_host_name",
        "db_port" : "database_port",
        "db_user" : "database_user_name",
        "db_pword": "database_password",
        "db_database" : "database_name",
        "db_appname" : "application_name_to_set" [OPTIONAL]
    }

"""

import psycopg2


class DBUtils:
    """Class for Database Access - Uses pgsql for now"""

    __db_host = None
    __db_port = None
    __db_user = None
    __db_pword = None
    __db_database = None
    __db_appname = None
    __connection = None
    __cursor = None
    __autocommit = None

    def __init__(self, conf, autocommit = True):
        # TODO: Add logging and error handling
        config = conf
        self.__db_host = config.get('db_host', None)
        self.__db_port = config.get('db_port', None)
        self.__db_user = config.get('db_user', None)
        self.__db_pword = config.get('db_pword', None)
        self.__db_database = config.get('db_database', None)
        self.__db_appname = config.get('db_appname', 'Test Application')
        self.__autocommit = autocommit

    def __enter__(self):
        # TODO: Add error handling, logging
        self.__connection = psycopg2.connect(host=self.__db_host,
                                             port=self.__db_port,
                                             user=self.__db_user,
                                             password=self.__db_pword,
                                             dbname=self.__db_database)
        self.__connection.autocommit = self.__autocommit
        self.__cursor = self.__connection.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # If autocommit is not set, the explicitly call commit
        if not self.__autocommit:
            self.__connection.commit()

        if self.__cursor.closed is False:
            self.__cursor.close()
        self.__connection.close()
    
    def execute_bulk_insert_aws_pricing(self, query, data=None, arg_str=None):
        """Run a DML statement.

        Args:
            query: The DML statement to be executed.
            data: The data to be used for bulk parametrized query
        """
        data_str = ','.join(self.__cursor.mogrify(arg_str, tuple(row[1:])).decode('utf-8') for row in data.itertuples())
        self.__cursor.execute(query + data_str)

    def execute_nonquery(self, query, data=None):
        """Run a DML statement.

        Args:
            query: The DML statement to be executed.
            data[Optional]: The data to be used for parametrized query
        """
        self.__cursor.execute(query, data)

    def execute_query(self, query, data=None):
        """Run a SELECT statement.

        Args:
            query: The SELECT statement to be executed
            data[Optional]: The data to be used for parametrized query

        Returns:
            Returns the result as dictionary
        """
        # TODO: Add logging and error handling
        self.__cursor.execute(query, data)
        columns = self.__cursor.description
        result = \
            [{columns[index][0]:column for
              index, column in enumerate(value)}
             for value in self.__cursor.fetchall()]
        return result

    def get_rowcount(self):
        """Returns the rowcount

        Returns:
            Returns the row count of the last query.
        """
        return self.__cursor.rowcount

    def get_row(self):
        """Returns the rowcount

        Returns:
            Returns the row count of the last query.
        """
        return self.__cursor.fetchall()

    def get_rowid(self):
        """Returns the row id

        Returns:
            Returns the row id of the last query.
        """
        return self.__cursor.fetchone()
        
    @property
    def query(self): return self.__cursor.query

    @property
    def connection(self): return self.__connection
