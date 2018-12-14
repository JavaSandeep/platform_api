"""Module to abstract S3 Access"""

import boto3
import os
import pandas as pd
import io


class S3Utils:
    """Class for S3 Access"""

    def __init__(self):
        pass

    def __enter__(self):
        self.__s3_client = boto3.client('s3')
        return self 

    def __exit__(self, exc_type, exc_value, traceback):pass

    def put_object(self, bucket_name, object_name, file_name):
        if not os.path.exists(file_name):
            raise "Local file {0} not found".format(file_name)
        self.__s3_client.upload_file(file_name, bucket_name, object_name)
        return True

    def put_object_bytes(self, bucket_name, object_name, data):
        self.__s3_client.put_object(Body=data, Bucket=bucket_name, Key=object_name)
        return True

    def get_object(self, bucket_name, object_name, file_name):
        """
        Reads an s3 object and saves it as a file (locally)
        :param bucket_name: The name of s3 bucket to read from
        :param object_name: The name of object(Key Name) to read
        :param file_name: The name of (local) file, where to save the object
        :return: True on success
        """
        try:
            self.__s3_client.download_file(bucket_name, object_name, file_name)
        except Exception as ex:
            raise ex
        
        if not os.path.exists(file_name):
            raise "Could not download file to {0}".format(file_name)
        return True

    def get_csv_df(self, bucket_name, object_name):
        """
        Read a CSV file from S3 and return as pandas dataframe
        :param bucket_name: The name of s3 bucket where the csv lives
        :param object_name: The name of object to read(Key Name)
        :return: Dataframe on success, None otherwise
        """
        try:
            obj = self.__s3_client.get_object(Bucket=bucket_name, Key=object_name)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8', low_memory=False)
            return df
        except Exception as ex:
            raise ex

    def get_csv_df_with_option(self, bucket_name, object_name, seperator=',', header=None, nafilter=False):
        """
        Read a CSV file from S3 and return as pandas dataframe
        :param bucket_name: The name of s3 bucket where the csv lives
        :param object_name: The name of object to read(Key Name)
        :return: Dataframe on success, None otherwise
        """
        try:
            obj = self.__s3_client.get_object(Bucket=bucket_name, Key=object_name)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8', low_memory=False, sep=seperator,
                                        header=header, na_values='', na_filter=nafilter)
            return df
        except Exception as ex:
            raise ex
