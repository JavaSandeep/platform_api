# -*- coding: utf-8 -*-
"""Class contains utilities for importing entity
This is property of CloudChomp

Functions contained:
1. Get config details using config id
2. Getting import configuration details using config id and file type
3. Updating collects table for collect that is being processed
4. Getting global configuration for an organization
5. Getting default configuration for specific config_type
6. Create mapped names dictionaries for renaming dataframe
7. Execute queries with parameters and skeleton queries
4. Validate CSV against mapped headers
"""


def bootstrap():
    """Initialization function for the script
    """
    import os
    import sys
    global ANALYTICS_HOME, ANALYTICS_CONFIG
    if 'ANALYTICS_NEXTGEN_HOME' in os.environ:
        ANALYTICS_HOME = os.environ['ANALYTICS_NEXTGEN_HOME']
        if ANALYTICS_HOME is None:
            print('CRITICAL ERROR: Environment Variable {0} not set. Exit.'.format(
                'ANALYTICS_NEXTGEN_HOME'))
            sys.exit()
        if not os.path.exists(ANALYTICS_HOME):
            print('CRITICAL ERROR: Analytics Home path({0}) does not exist. Exit.'.format(
                ANALYTICS_HOME))
            sys.exit()
    else:
        print('CRITICAL ERROR: Environment Variable {0} not set. Exit.'.format(
            'ANALYTICS_NEXTGEN_HOME'))
        sys.exit()
    if 'ANALYTICS_NEXTGEN_CONFIG' in os.environ:
        ANALYTICS_CONFIG = os.environ['ANALYTICS_NEXTGEN_CONFIG']
        if ANALYTICS_CONFIG is None:
            print("CRITICAL ERROR: Environment Variable {0} not set. Exit.".format("ANALYTICS_NEXTGEN_CONFIG"))
            sys.exit()
        if not os.path.exists(ANALYTICS_CONFIG):
            print("CRITICAL ERROR: Analytics Config path({0}) does not exist. Exit.".format(ANALYTICS_CONFIG))
            sys.exit()
    else:
        print("CRITICAL ERROR: Environment Variable {0} not set. Exit.".format("ANALYTICS_NEXTGEN_CONFIG"))
        sys.exit()


bootstrap()
import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime
sys.path.append(os.path.join(os.environ['ANALYTICS_NEXTGEN_HOME'], 'utils', 'logutils'))
from logutils import LogUtils
sys.path.append(os.path.join(os.environ['ANALYTICS_NEXTGEN_HOME'], 'utils', 'dbutils'))
from dbutils import DBUtils


class ImportUtils:
    """Utility class for importing files"""

    __uniquely_identifying_columns = {
        "license": ["Product Name", "Product Edition", "Product Version", "License Vendor"]
    }

    # EXCLUDE THESE COLUMNS FROM DATA VALIDATION
    __exclusion_list = {
        "license": ["Product Version", "Product Edition"]
    }

    __validation_processes = {
        "EMPTY DATAFRAME VALIDATION": "PASSED",
        "NUMBER OF COLUMNS MISMATCHED VALIDATION": "PASSED",
        "DATA DUPLICATE VALIDATION": "FAILED",
        "DATA TYPE VALIDATION": "FAILED",
        "MANDATORY COLUMNS MISSING VALIDATION": "FAILED",
        "MANDATORY VALUES MISSING VALIDATION": "FAILED"
    }

    def __init__(self, config_file=None):
        """
        Constructor for import utilities
        """
        if config_file is None:
            config_file_path = ANALYTICS_CONFIG
        else:
            config_file_path = config_file

        # Check if configuration file exists
        if not os.path.exists(config_file_path):
            raise Exception(
                "Configuration file {0} does not exist".format(config_file_path))
        # Load the configuration file
        try:
            with open(config_file_path) as config_json:
                self.__config = json.load(config_json)
        except Exception as e:
            raise Exception(e)

        # Initialize the logger
        current_filename = os.path.basename(__file__)
        try:
            self.__logger = LogUtils(
                current_filename, self.__config['logger'], False).get_logger()
        except Exception as e:
            raise Exception(e)

    def get_config_details(self, collect_id):
        """
        Function to get config details from warehouse
        :param collect_id: collect id that was used in collects table
        :return: returns configuration details if success, else None
        """
        self.__logger.debug("Executing function get_config_details for collect_id: {0}".format(collect_id))
        with DBUtils((self.__config['application'])) as dbConn:
            collect_row_result = dbConn.execute_query(self.__collect_details_query, (collect_id,))
            self.__logger.debug("Ran query: {0}".format(dbConn.query))

        if not collect_row_result:
            self.__logger.error("No data found in collects table for collect_id: {0}".format(collect_id))
            self.__logger.info('Exiting script.')
            return None
        # LIST OF DICT, GETTING FIRST COLUMN
        collect_row = collect_row_result[0]
        self.__logger.debug("Result: {0}".format(str(collect_row)))
        return collect_row

    def get_import_configuration(self, config_id, file_type):
        """
        Function to get import configuration
        :param config_id: config id that was used in collects
        :param file_type: Machine, Storage, Physical Machine or Virtual Machine
        :return: configuration if true, else None
        """
        self.__logger.debug("Executing function get_import_configuration")
        if config_id == 0 and file_type == 'Virtual Machine':
            query = "select get_config_json->>'vm' as mapped_headers,'t' as is_first_row_header," \
                    " 'virtual_machine' as import_entity_type, 0 as headers_count, 'comma' as " \
                    "column_separator from get_config_json('default','MappedHeaders')"
        elif config_id == 0 and file_type == 'Physical Machine':
            query = "select get_config_json->>'physical'  as mapped_headers,'t' as is_first_row_header," \
                    " 'physical_machine' as import_entity_type, 0 as headers_count, 'comma' as " \
                    "column_separator from get_config_json('default','MappedHeaders')"
        elif config_id == 0 and file_type == 'License':
            query = "select get_config_json->>'license'  as mapped_headers,'t' as is_first_row_header," \
                    " 'license' as import_entity_type, 0 as headers_count, 'comma' as " \
                    "column_separator from get_config_json('default','MappedHeaders')"
        else:
            query = "select is_first_row_header, import_entity_type, mapped_headers, headers_count, " \
                    "column_separator, update_only from import_configurations where config_id=%s"

        with DBUtils((self.__config['application'])) as dbConn:
            if config_id:
                import_configuration_result = dbConn.execute_query(query,(config_id,))
            else:
                import_configuration_result = dbConn.execute_query(query)
            self.__logger.debug("Ran query: {0}".format(dbConn.query))
        
        if not import_configuration_result:
            error = "Configuration not found"
            self.__logger.error("Configuration not found for config id: {}".format(config_id))
            errors = {"error_msg": [error], "identifiers":['']}
            return False, errors
        
        import_configuration = import_configuration_result[0]
        self.__logger.debug("Result import_configuration: {0}".format(str(import_configuration)))

        return True, import_configuration

    def update_collects(self, collect_id, error_list, status, status_only=False):
        """
        Utility function to update collects table status against collect id
        :param collect_id: Collect id of entry in collects table
        :param error_list: List of errors if there, that occurred while processing error
        :param status: status message after processing the collect
        :param status_only: flag to determine only status needs to be updated
        :return: True if collects is updated, else False
        """
        self.__logger.info("Executing update_collects function")
        try:
            if status_only:
                update_query = "update collects set status=%s where id=%s"
                self.__logger.debug('Running query: %s' % update_query % (status, collect_id))
                with DBUtils((self.__config['application'])) as dbConn:
                    dbConn.execute_nonquery(update_query, (status, collect_id))
            else:
                update_query = "update collects set status=%s, status_message=%s where id=%s"
                self.__logger.debug('Running query: %s' % update_query % (status, error_list, collect_id))
                with DBUtils((self.__config['application'])) as dbConn:
                    dbConn.execute_nonquery(update_query,
                                            (status, json.dumps({'errors': error_list}),
                                             collect_id))
            return True
        except Exception as ex:
            self.__logger.error("Could not update collects table {0}".format(str(ex)))
            return False
    
    def get_global_settings(self, organization_id):
        """
        Function to get default settings  from configurations table.
        :param organization_id: String
        :return: Default/Global Settings and True if success, else False and error message
        """
        q_resource_settings = """select settings from resource_settings rs, organizations org where rs.resource_id = org.id
                                        and resource_type = 'ORGANIZATION' and setting_group = 'global_settings'
                                        and org.orgid = %s """

        self.__logger.info("Getting global_settings")

        with DBUtils(self.__config['application']) as db_conn:
            self.__logger.debug('Connected to database')
            self.__logger.debug("Running Query: %s" % q_resource_settings % (organization_id,))
            result = db_conn.execute_query(q_resource_settings, (organization_id,))
            self.__logger.debug("Result  received: %s" % result)
        # Checking global_settings for the given organization
        if result is None or len(result) == 0:
            self.__logger.warning("No global_settings set for organization : %s." % organization_id)
            self.__logger.debug("Using default settings")
            default_query = """select * from get_config_json( %s, %s)"""
            try:
                with DBUtils(self.__config['application']) as db_conn:
                    self.__logger.debug('Connected to database')
                    self.__logger.debug("Running Query: %s" % default_query % (organization_id, "DefaultSettings"))
                    default_settings = db_conn.execute_query(default_query, (organization_id, "DefaultSettings"))
                    self.__logger.debug("Result  received: %s" % default_settings)

                    # Sanity Check
                    if default_settings is None or len(default_settings) == 0:
                        error = "No global settings found"
                        errors = {"error_msg": [error], "identifiers":['']}
                        self.__logger.error(error)
                        return False, errors
                settings = default_settings[0].get('get_config_json')
                self.__logger.debug("Result global_settings: {0}".format(str(settings)))
                return True, settings
            except Exception as ex:
                error = "Could not get default settings"
                errors = {"error_msg": [error], "identifiers":['']}
                self.__logger.error("Could not get default settings. Exception: {0}".format(ex))
                return False, errors
        else:
            settings = result[0].get('settings')
            self.__logger.debug("Result global_settings: {0}".format(str(settings)))
            return True, settings

    def get_default_configurations(self, organization_id, config_name):
        """
        Get configuration against organization for a configuration name
        :param organization_id: Organization id against which configuration is required
        :param config_name: Name of configuration
        :return: True and config dict if success, else false and error message
        """
        with DBUtils((self.__config['application'])) as dbConn:
                result = dbConn.execute_query('select get_config_json as default_config from '
                                              'get_config_json(%s, %s)', (organization_id, config_name))
                self.__logger.debug("Ran query: {0}".format(dbConn.query))
        if not result:
            error_msg = "No default configuration found in configurations table for config type: 'FilterFields'"
            errors = {"error_msg": [error_msg], "identifiers":['']}
            self.__logger.error(error_msg)
            return False, errors
        result_config = result[0]
        self.__logger.debug("Result FilterFields: {0}".format(str(result_config)))

        return True, result_config

    def get_renaming_dict(self, mapper_dict_list, from_key, to_key):
        """
        Funtion to create pandas renaming dictionary using keys and values
        :param mapper_dict_list: List containing dicts which contains mapping
        :param from_key: key containing name of column to be changed
        :param to_key: key containing name of column to be changed into
        :return: dictionary containing mapping, else None
        """
        result_dict = {}
        for each_row in mapper_dict_list:
            result_dict[each_row.get(from_key)] = each_row.get(to_key)
        self.__logger.debug("Renaming dictionary: {0}".format(str(result_dict)))
        
        return result_dict
    
    def execute_query(self, built_query, parameters=None):
        """
        Function to execute query with parameters
        :param built_query: Query to execute
        :param parameters: parameters to be used
        :return: result of query, or empty list
        """
        with DBUtils((self.__config['application'])) as dbConn:
            if not parameters:
                result = dbConn.execute_query(built_query)
            else:
                result = dbConn.execute_query(built_query, parameters)                
            self.__logger.debug("Ran query: {0}".format(dbConn.query))
        return result
    
    def is_date_greater(self, newer_date, older_date):
        """
        Function to compare two dates
        :param newer_date: newer date in date type 
        :param older_date: older date in date type
        :return: True if newer date is greater, else False
        """
        date_list = [newer_date, older_date]
        sep_list = ['-', '/', ':', '.', '|']
        date_obj_list = []

        seperator = None
        for each_sep in sep_list:
            if each_sep in older_date:
                seperator = each_sep
                break

        for i_date in date_list:
            if seperator:
                parse_format = "%m{0}%d{0}%Y".format(seperator)
                date_obj_list.append(datetime.strptime(i_date, parse_format))
            else:
                return False

        if date_obj_list[0] > date_obj_list[1]:
            return True
        else:
            return False

    def validate_csv(self, file_as_df, mapped_headers, table_specific_headers, default_entity_type_config, update_only, import_entity_type):
        """
        Function to validate csv
        :param file_as_df: csv as dataframe
        :param mapped_headers: Headers mapped along with configuration
        :param table_specific_headers: Headers mapped along with database table
        :param default_entity_type_config: configuration for (machine, storage, virtual machine, physical machine)
        :param update_only: update only case
        :param import_entity_type: configuration type (machine, storage, virtual machine, physical machine)
        :return: list of errors if any
        """
        errors_list = []
        is_not_empty = True
        is_header_length_not_mismatched = True
        #checks whether imported file is empty
        #if empty, returns error
        if file_as_df.shape[0] == 0:
            errors_list.append({
                "error_msg": ["No data found in csv file"],
                "identifiers": ['']
            })
            self.__logger.error('No data found in csv file')
            self.__validation_processes["EMPTY DATAFRAME VALIDATION"] = "FAILED"
            is_not_empty = False
         
        # Check if the number of columns matches the configuration
        # Can happen when columns are ignored or added in the import file
        # Can also happen if wrong column separator is used or wrong file is used for a given config
        # If so stop the script
        if len(mapped_headers) != len(file_as_df.columns):
            errors_list.append({
            "error_msg": ["""The number of columns on file and config do not match. Some things that can cause this:\n
            #1. Incorrectly configured file \n
            #2. Use of incorrect column separator. eg, '|' instead of ','\n
            #3. Wrong configuration """],
            "identifiers": ['']
            })
            self.__logger.error(errors_list)
            self.__validation_processes["NUMBER OF COLUMNS MISMATCHED VALIDATION"] = "FAILED"
            is_header_length_not_mismatched = False

        # Get the col for unique identifier to log errors, gets the column index (to compensate for files w/o headers)
        if is_not_empty and is_header_length_not_mismatched:
            drop_duplicates_col_list = []
            if import_entity_type=="virtual_machine":
                identifier_col = list(filter(lambda x: x.get('title') == "VM Identifier", mapped_headers))
                if not update_only:
                    host_identifier_col = list(filter(lambda x: x.get('title') == "Host", mapped_headers))
                if identifier_col:
                    identifier_col_index = identifier_col[0].get('index')
                    if not update_only:
                        host_identifier_col_index = host_identifier_col[0].get('index')
                        to_string_cols = [file_as_df.columns[identifier_col_index], 
                                        file_as_df.columns[host_identifier_col_index]]
                    else:
                        to_string_cols = [file_as_df.columns[identifier_col_index]]
                    drop_duplicates_col = file_as_df.columns[identifier_col_index]
                    #Before identifiers were not type cast as string which was causing problem when
                    #identifier has only numeric values. So, we are type casting identifier column to string type.
                    file_as_df[to_string_cols] = file_as_df[to_string_cols].astype(str)
                    drop_duplicates_col_list = [drop_duplicates_col]
            elif import_entity_type=="physical_machine":
                identifier_col = list(filter(lambda x: x.get('title') == "Machine Identifier", mapped_headers))
                if identifier_col:
                    identifier_col_index = identifier_col[0].get('index')
                    drop_duplicates_col = file_as_df.columns[identifier_col_index]
                    #Before identifiers were not type cast as string which was causing problem when
                    #identifier has only numeric values. So, we are type casting identifier column to string type.
                    file_as_df[drop_duplicates_col] = file_as_df[drop_duplicates_col].astype(str)
                    drop_duplicates_col_list = [drop_duplicates_col]
            elif import_entity_type=="license":
                uniquely_identifying_columns = self.__uniquely_identifying_columns.get(import_entity_type)
                
                drop_duplicates_col_list = []
                for each_header in mapped_headers:
                    for col in uniquely_identifying_columns:
                        if col == each_header.get("title"):
                            drop_duplicates_col_list.append(file_as_df.columns[each_header.get("index")])
            else:
                self.__logger.error("Unique identifier column is not valid, ie, import_entity_type mismatched")
                errors_list.append({
                    "error_msg":["Unique identifier column is not valid."],
                    "identifiers": ['']
                })
                self.__logger.error(errors_list)
            if drop_duplicates_col_list:
                self.__logger.debug("Validating for duplicates in csv")
                self.__logger.debug("Duplicate identifier: {0}".format(str(drop_duplicates_col_list)))
                dataframe_after_dropping_duplicates = file_as_df.drop_duplicates(subset=drop_duplicates_col_list,
                                                                                keep='first')
                unique_dropped_column = file_as_df[file_as_df.duplicated(subset=drop_duplicates_col_list, keep=False)]
                unique_dropped_column.drop_duplicates(subset=drop_duplicates_col_list, keep='first', inplace=True)
                duplicate_list=["Row "+str(x+1) for x in list(unique_dropped_column.index.values)]
                is_data_duplicate = False
                if len(file_as_df) > len(dataframe_after_dropping_duplicates):
                    self.__logger.error("Duplicates rows found {0}".format(str(duplicate_list)))
                    errors_list.append({
                        "error_msg": ["Duplicate Data in CSV"],
                        "identifiers": duplicate_list 
                    })
                    is_data_duplicate = True

                if not is_data_duplicate:
                    self.__logger.debug("No duplicates found.")
                    self.__validation_processes["DATA DUPLICATE VALIDATION"] = "PASSED"
            else:
                errors_list.append({
                    "error_msg": ["No unique columns found"],
                    "identifiers": ['']
                })
            system_headers = list(filter(lambda x: ("System"==x.get('type') and '' != x.get('title')), mapped_headers))
            number_of_rows, number_of_columns = file_as_df.shape
            
            # VALIDATION FOR PARTIAL CONTAINMENT OF COLUMNS
            if import_entity_type=="license":
                self.__logger.debug("Validating csv for partial containment of columns")
                present_list = []
                absent_list = []
                columns_to_examine = ["CAL Type", "Number of CALs", "Total CAL Cost"]
                for cols in columns_to_examine:
                    column_list = list(filter(lambda x: x.get("title") == cols, system_headers))
                    if not column_list:
                        absent_list.append(cols)
                    else:
                        present_list.append(cols)
                # IF ANY COLUMN IS PRESENT BUT NOT ALL COLUMN ARE ABSENT
                if present_list and absent_list:
                    self.__logger.error("Present columns: {0}".format(str(present_list)))
                    self.__logger.error("Absent columns: {0}".format(str(absent_list)))
                    errors_list.append({
                        "error_msg": ["Some columns {0} are absent".format(str(absent_list))],
                        "identifiers": absent_list
                    })

            # VALIDATION FOR LICENSE TYPE AND MISSING VALUES
            if import_entity_type == "license":
                self.__logger.debug("Validating csv for missing values regarding license types")
                license_type_list = list(filter(lambda x: x.get('title') == 'License Type', system_headers))
                contains_type_column = True
                if license_type_list:
                    license_type = file_as_df.columns[license_type_list[0].get('index')]
                else:
                    contains_type_column=False
                file_as_df_temp = file_as_df.copy(deep=True)

                file_as_df_temp = file_as_df_temp.drop(file_as_df_temp[file_as_df_temp[license_type].isnull()].index)

                is_not_empty = True
                if file_as_df_temp is None or len(file_as_df_temp) == 0:
                    errors_list.append({
                                "error_msg": ["License Type is cannot be empty"],
                                "identifiers": ['License Type']
                            })
                    is_not_empty = False
                
                if contains_type_column and is_not_empty:
                    file_as_df_temp[license_type] = file_as_df_temp[license_type].astype(str).str.lower()
                    self.__logger.debug("Checking file regarding license type")
                    number_of_cal_col_list = list(filter(lambda x: x.get('title') == 'Number of CALs', system_headers))
                    total_cal_cost_col_list = list(filter(lambda x: x.get('title') == 'Total CAL Cost', system_headers))
                    cal_type_col_list = list(filter(lambda x: x.get('title') == 'CAL Type', system_headers))
                    core_pack_col_list = list(filter(lambda x: x.get('title') == 'License Core Pack', system_headers))

                    is_NOC_present = True if number_of_cal_col_list else False
                    is_TCC_present = True if total_cal_cost_col_list else False
                    is_CT_present = True if cal_type_col_list else False
                    is_LCP_present = True if core_pack_col_list else False
                    
                    # Abbreviations are used to keep code short and clean
                    # NOC represents Number of CALs
                    # TCC represents Total CAL Cost
                    # CT represents CAL Type
                    # LCP represents License Core Pack
                    if is_NOC_present:
                        number_of_cal_col = file_as_df_temp.columns[number_of_cal_col_list[0].get('index')]
                    if is_TCC_present:
                        total_cal_cost_col = file_as_df_temp.columns[total_cal_cost_col_list[0].get('index')]
                    if is_CT_present:
                        cal_type_col = file_as_df_temp.columns[cal_type_col_list[0].get('index')]
                    if is_LCP_present:
                        core_pack_col = file_as_df_temp.columns[core_pack_col_list[0].get('index')]
                    
                    if not (is_NOC_present or is_TCC_present or is_CT_present or is_LCP_present):
                        error_message = "License type is present but None of Number of CALs, Total CAL Cost, CAL Type, License Core Pack are present"
                        self.__logger.error(error_message)
                        errors_list.append({
                                "error_msg": [error_message],
                                "identifiers": ['']
                            })
                    else:
                        file_as_df_temp['error_msg'] = np.nan

                        self.__logger.debug("Checking if Per Core columns are present or not")
                        if is_LCP_present:
                            file_as_df_temp['error_msg'] = np.where(file_as_df_temp[license_type]=="per core", np.where(is_LCP_present & (file_as_df_temp[core_pack_col].isnull()),
                                                                'Missing license core pack', np.nan), np.nan)
                        if is_NOC_present and is_TCC_present and is_CT_present:
                            self.__logger.debug("Checking if Server-CAL columns are present or not")
                            file_as_df_temp['error_msg'] = np.where(file_as_df_temp[license_type]=="server-cal", np.where(is_NOC_present & is_TCC_present &\
                                                                is_CT_present & (file_as_df_temp[number_of_cal_col].isnull()) &\
                                                                (file_as_df_temp[total_cal_cost_col].isnull()) & (file_as_df_temp[cal_type_col].isnull()),
                                                                'Missing either of Number of CALs, Total CAL cost or CAL type', file_as_df_temp.error_msg),
                                                                file_as_df_temp.error_msg)
                        if is_LCP_present and is_NOC_present and is_TCC_present and is_CT_present:
                            self.__logger.debug("Checking if Per Core-CAL columns are present or not")
                            file_as_df_temp['error_msg'] = np.where(file_as_df_temp[license_type]=="per core-cal", np.where(is_LCP_present & is_NOC_present &\
                                                                is_TCC_present & is_CT_present & (file_as_df_temp[core_pack_col].isnull()) &\
                                                                (file_as_df_temp[number_of_cal_col].isnull()) &\
                                                                (file_as_df_temp[total_cal_cost_col].isnull()) & (file_as_df_temp[cal_type_col].isnull()),
                                                                'Missing either of License Core Pack, Number of CALs, Total CAL cost or CAL type', file_as_df_temp.error_msg),
                                                                file_as_df_temp.error_msg)
                        file_as_df_temp = file_as_df_temp.reset_index().rename(columns={'index':'identifiers'})
                        self.__logger.debug("Converting index to row number")
                        file_as_df_temp['identifiers'] = file_as_df_temp['identifiers']+1
                        file_as_df_temp['identifiers'] = 'Row ' + file_as_df_temp['identifiers'].astype(str)
                        
                        self.__logger.debug("Dropping rows with empty error messages")
                        file_as_df_temp = file_as_df_temp.drop(file_as_df_temp[file_as_df_temp['error_msg']=='nan'].index)

                        if len(file_as_df_temp) > 0:
                            encountered_errors = file_as_df_temp[['identifiers', 'error_msg']].to_dict('records')
                            self.__logger.debug("Converting error to list of errors")
                            temp_errors_list = []
                            for each_err in encountered_errors:
                                for each_key in each_err:
                                    each_err[each_key] = [each_err[each_key]]
                                temp_errors_list.append(each_err)
                            encountered_errors = temp_errors_list
                            temp_errors_list = None

                            self.__logger.warning("Encountered errors: {0}".format(str(encountered_errors)))
                            errors_list.extend(encountered_errors)
                        file_as_df_temp = None
                else:
                    self.__logger.error("Does not contains License type (Mandatory) column")
                    errors_list.append({
                                "error_msg": ["Does not contains License type column"],
                                "identifiers": ['']
                            })

            # VALIDATION FOR PURCHASE DATE AND EXPIRY DATE
            if import_entity_type == "license":
                self.__logger.debug("Validating csv dates for license")
                contains_date_column = True
                purchase_date_list = list(filter(lambda x: x.get('title') == 'Purchase Date', system_headers))
                if purchase_date_list:
                    purchase_date_colname = file_as_df.columns[purchase_date_list[0].get('index')]
                else:
                    contains_date_column = False

                expiry_date_list = list(filter(lambda x: x.get('title') == 'Expiry Date', system_headers))
                if expiry_date_list:
                    expiry_date_colname = file_as_df.columns[expiry_date_list[0].get('index')]
                else:
                    contains_date_column = False
                
                if contains_date_column:
                    file_as_df = file_as_df.drop(file_as_df[(file_as_df[purchase_date_colname].isnull())|(file_as_df[expiry_date_colname].isnull())].index)
                    number_of_rows, number_of_columns = file_as_df.shape
                    
                    self.__logger.debug("Purchase column name: {0}".format(purchase_date_colname))
                    self.__logger.debug("Expiry column name: {0}".format(expiry_date_colname))
                    for row_index in range(0, number_of_rows):
                        if not self.is_date_greater(file_as_df[expiry_date_colname][row_index], file_as_df[purchase_date_colname][row_index]):
                            errors_list.append({
                                "error_msg": ["Purchase date older than expiry date"],
                                "identifiers": ["Row {0}".format(str(row_index+1))]
                            })
                            self.__logger.warning("Row {0} has purchase date older than expiry date".format(str(row_index+1)))
                elif not update_only and not contains_date_column:
                    self.__logger.error("Does not contains date columns")
                    errors_list.append({
                                "error_msg": ["Does not contains date columns"],
                                "identifiers": ['']
                            })


            for header in system_headers:
                # ITERATING THROUGH ALL MAPPED SYSTEM HEADERS
                # i.e. SYSTEM HEADERS THAT WERE USED DURING MAPPING
                default_header = list(filter(lambda x: header.get('title') == x.get('title'), default_entity_type_config))
                
                if not default_header:
                    self.__logger.error("{0} system headers not in default entity config.".format(header.get("title")))
                    errors_list.append({
                         "error_msg": ["Header not in entity. Internal Error"],
                         "identifiers": ['']
                     })
                    raise Exception("{0} system headers not in default entity config.".format(header.get("title")))

                header_dict = default_header[0]

                index_in_csv = header.get("index")
                column_name = header.get("title")
                csv_column_name = file_as_df.columns[index_in_csv]
                column_is_required = header_dict.get('is_required')
                # column_data_type = file_as_df.iloc[:, index_in_csv].dtype
                column_data_type = header_dict.get("col_type")

                self.__logger.info('Validating for column: {0}'.format(column_name))

                if not default_header:
                    self.__logger.error("System headers not in default entity config.")
                    errors_list.append({
                        "error_msg": ["Header not in entity. Internal Error"],
                        "identifiers": ['']
                    })
                    raise Exception("System headers not in default entity config.")
                
                # CHECK FOR IF COLUMN EXISTS IN CSV
                mandatory_columns_present=True
                if column_is_required:
                    if index_in_csv > number_of_columns:
                        self.__logger.error("{0} column not found in csv".format(column_name))
                        errors_list.append({
                            "error_msg": ["{0} column not found in csv".format(column_name)],
                            "identifiers": ['']
                        })
                        mandatory_columns_present=False
                if mandatory_columns_present:
                    self.__validation_processes["MANDATORY COLUMNS MISSING VALIDATION"] = "PASSED"

                
                # CHECK IF MANDATORY COLUMN HAS MISSING VALUE
                mandatory_values_not_missing=True
                if column_is_required:
                    empty_rows = file_as_df.loc[file_as_df.iloc[:, index_in_csv].isnull(),]
                    if(empty_rows.shape[0] > 0):
                        errors_list.append({
                            "error_msg":["Values missing in {0} column.".format(column_name)],
                            "identifiers": [column_name]
                        })
                        mandatory_values_not_missing=False
                        self.__validation_processes["MANDATORY VALUES MISSING VALIDATION"] = "FAILED"
                if mandatory_values_not_missing:
                    self.__validation_processes["MANDATORY VALUES MISSING VALIDATION"] = "PASSED"
                
            # COERCING FILE TYPES TO CONVERT DATA TYPES
            # self.__logger.debug("Filling NA with matching data types")
            # file_as_df = self.fill_na_with_matching_datatype(file_as_df, table_specific_headers, default_entity_type_config)
            # if file_as_df is None or len(file_as_df) == 0:
            #     self.__logger.error("Could not fill NA values with matching data types.")
            #     errors_list.append({"error_msg": ["Internal Error"], "identifiers": ['']})
            # file_as_df[drop_duplicates_col_list] = file_as_df[drop_duplicates_col_list].astype(str)

            exclusion_list = self.__exclusion_list.get("license", [])
            
            for header in system_headers:
                # COLUMN DATA TYPE VALIDATION
                default_header = list(filter(lambda x: header.get('title') == x.get('title'), default_entity_type_config))
                
                if not default_header:
                    self.__logger.error("{0} system headers not in default entity config.".format(header.get("title")))
                    errors_list.append({
                         "error_msg": ["Header not in entity. Internal Error"],
                         "identifiers": ['']
                     })
                    raise Exception("{0} system headers not in default entity config.".format(header.get("title")))


                header_dict = default_header[0]
                index_in_csv = header.get("index")
                column_name = header.get("title")
                csv_column_name = file_as_df.columns[index_in_csv]
                column_data_type = header_dict.get("col_type")

                self.__logger.debug("Data type validation starts")
                column_df = file_as_df[csv_column_name]
                is_data_type_incorrect = False
                if column_data_type in ["number", "float", "integer"]:
                    # FOR SOME STRING OR OBJECT COLUMN
                    # ITERATING THROUGH ALL ROWS
                    for row_index in range(0, number_of_rows):
                        is_row_numeric = self.is_number(column_df[row_index])

                        if not is_row_numeric:
                            # ELEMENT IS NEITHER INTEGER OR FLOAT
                            self.__logger.error("{0} column, Row {1} has invalid data".format(column_name, row_index+1))
                            errors_list.append({
                                "error_msg":["{0} column, Row {1} has invalid data".format(column_name, row_index+1)],
                                "identifiers": [column_name]
                            })
                            is_data_type_incorrect = True
                        if is_row_numeric and column_df[row_index] < 0:
                            self.__logger.error("{0} column, Row {1} has negative value".format(column_name, row_index+1))
                            errors_list.append({
                                "error_msg":["{0} column, Row {1} has negative value".format(column_name, row_index+1)],
                                "identifiers": [column_name]
                            })
                            is_data_type_incorrect = True
                    if is_data_type_incorrect:
                        self.__validation_processes["DATA TYPE VALIDATION"] = "PASSED"
                elif column_data_type in ["text", "string", "datetime"]:
                    # FOR SOME STRING COLUMNS
                    column_df.fillna(value='', inplace=True)
                    if column_name not in exclusion_list:
                        for row_index in range(0, number_of_rows):
                            # ITERATE THROUGH EACH ROWS AND CHECK IF IT COULD BE CONVERTED NUMBER
                            if (type(column_df[row_index]) != str) and (self.is_number(column_df[row_index])):
                                self.__logger.error("{0} column, Row {1} has numeric value".format(column_name, row_index+1))
                                errors_list.append({
                                    "error_msg":["{0} column, Row {1} has numeric value".format(column_name, row_index+1)],
                                    "identifiers": [column_name]
                                })
                                is_data_type_incorrect = True
                    if is_data_type_incorrect:
                        self.__validation_processes["DATA TYPE VALIDATION"] = "PASSED"
                else:
                    self.__logger.warning("Not Mandatory columns")
        self.__logger.debug("----REPORT----")
        self.__logger.debug(str(self.__validation_processes))
        self.__logger.info(errors_list)
        return errors_list

    def is_number(self, s):
        try:
            float(s)
            return True
        except ValueError:
            pass
    
        try:
            import unicodedata
            unicodedata.numeric(s)
            return True
        except (TypeError, ValueError):
            pass
    
        return False

    def fill_na_with_matching_datatype(self, file_dataframe, mapping_headers, required_headers):
        """
        Function to fill na of columns, compatible to it's data type
        :param file_dataframe: uploaded csv read as dataframe
        :param mapping_headers: headers mapped
        :param required_headers: validating json from filter fields
        :return: dataframe after NA values are filled and data types corrected, else None
        """
        try:
            for each_item in mapping_headers:
                default_header = list(filter(lambda x: each_item.get('title') == x.get('title'), required_headers))
                header_dict = default_header[0]
                column_type = header_dict.get("col_type")
                column_name = file_dataframe.columns[each_item.get('index')]

                if column_type in ["float"]:
                    file_dataframe[column_name] = pd.to_numeric(file_dataframe[column_name],
                                                                downcast='float',
                                                                errors='coerce')
                    file_dataframe[column_name].fillna(value=0.0, inplace=True)
                elif column_type in ["number", "integer"]:
                    file_dataframe[column_name] = pd.to_numeric(file_dataframe[column_name],
                                                                downcast='integer',
                                                                errors='coerce')
                    file_dataframe[column_name].fillna(value=0, inplace=True)
                elif column_type in ["string", "text"]:
                    file_dataframe[column_name].fillna(value='', inplace=True)
                    file_dataframe[column_name] = file_dataframe[column_name].astype(str)
            
            # STRIPING TRAILING WHITESPACES
            for col in file_dataframe.columns:
                if (np.issubdtype(file_dataframe[col].dtype, np.object)
                        and
                        not (
                            np.issubdtype(file_dataframe[col].dtype, np.integer)
                            or
                            np.issubdtype(file_dataframe[col].dtype, np.float)
                        )
                ):
                    file_dataframe[col] = file_dataframe[col].str.strip()
        except Exception as ex:
            self.__logger.error("Could not fill na values. Exception {0}".format(str(ex)))
            return None
        return file_dataframe

    __collect_details_query = """
    SELECT uploaded_file, organization_id, file_type, config_id 
    FROM collects 
    WHERE id=%s
    """
