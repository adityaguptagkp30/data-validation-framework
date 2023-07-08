# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###### Purpose: Utilities for every notebook
# MAGIC
# MAGIC ###### Parameter Info:
# MAGIC
# MAGIC ###### Revision History:
# MAGIC
# MAGIC |Date           |Author             |Description                                                                                                          |Execution Time          |
# MAGIC |---------------|:-----------------:|---------------------------------------------------------------------------------------------------------------------|------------------------|
# MAGIC |feb 24, 2023   |Aditya G           |Created this notebook for the functions which are globally required in other notebooks                               |                        |
# MAGIC |Mar 31, 2023    |Aditya G           |Created upsert_data_to_layer,read_filtered_delta_files,append_data_to_layer function for increamental data pull logic|                        |

# COMMAND ----------

# MAGIC %run ../Config/Config

# COMMAND ----------

# MAGIC %md
# MAGIC Importing Libraries and Packages

# COMMAND ----------

import re
import os
import adal
import msal
import json
import datetime
import xml.etree.ElementTree as ET
from delta.tables import DeltaTable
from typing import Dict, List, Tuple
from pepemailpkg import sendEmailGeneric
from pyspark.sql import DataFrame, SparkSession
from great_expectations.dataset import SparkDFDataset
from pyspark.sql.functions import when, col, upper, explode, lit

# COMMAND ----------

# Returns the current user who is running this file. used in the historicat table
current_user = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())['tags']['user']

# COMMAND ----------

# MAGIC %md
# MAGIC Helper functions

# COMMAND ----------

# Function to return the unique set of values.
def create_unique_list(_df, column):
    tables_list = _df.select(column).rdd.flatMap(lambda x: x).collect()
    return [*set(tables_list)]

# COMMAND ----------

# Function to read delta file
def read_delta_files(file_path):
    data_frame = spark.read.format("delta").load(file_path)
    return data_frame

# COMMAND ----------

# Function to return the values of the particular column
def get_first_value_from_dataframe_column(_df, column_name):
    for row in _df.rdd.collect():
        return row[column_name]

# COMMAND ----------

# Function to return the values of a particular column in the form of list.
def get_list_from_dataframe_column(_df, column_name):
    l1 = []
    for row in _df.rdd.collect():
        l1.append(row[column_name])
    return l1

# COMMAND ----------

# Function to return the spark dataframe from ADB and synapse
def get_data_from_tables(table_name):
    return spark.sql("SELECT * FROM " + table_name)

# COMMAND ----------

# Function to match the schema in audit DB for historical data.
def match_the_schema_with_audit_db(df):
    df = df.withColumn("ROW_CNT", df.ROW_CNT.cast('int'))
    df = df.withColumn("SRC_RSLT", df.SRC_RSLT.cast('int'))
    df = df.withColumn("DEST_RSLT", df.DEST_RSLT.cast('int'))
    df = df.withColumn("POST_RUN_RSLT", df.POST_RUN_RSLT.cast('int'))
    df = df.withColumn("PRE_RUN_RSLT", df.PRE_RUN_RSLT.cast('int'))
    df = df.withColumn("KPI_RSLT", df.KPI_RSLT.cast('int'))
    return df

# COMMAND ----------

# Function to read CSV files
def read_csv_files(file_path: str, delimiter_val: str):
    data_frame = spark.read.options(header="True"\
                                    ,delimiter=delimiter_val\
                                    ,multiLine="True"\
                                    ,escape='"')\
                            .csv(file_path)
    return data_frame


# Function to save the dataframe as parquet files into ADLS layer
def overwrite_data_to_layer(data_frame: DataFrame, location: str):
    data_frame.coalesce(1).write.format("delta").option("delta.columnMapping.mode", "name").option("overwriteSchema", "true").mode("overwrite").save(location)


# Function to remove existing Delta table folder in ADLS
def drop_folder_if_exists(folder_path):
    try:
        folder_list = dbutils.fs.ls(folder_path)
        if folder_list is not None:
            folder_exist_message = "Folder exists and dropped"
            dbutils.fs.rm(folder_path, True)
            print(f"Dropped folder: {folder_path}")
    except:
        folder_exist_message = "Folder does not exist"
    return folder_exist_message


# Function to return dataframe schema to get the columns info for renmaing columns to standardized format
def get_schema_from_df(data_frame: DataFrame) -> Dict[str, str]:  # pylint: disable=unused-variable
    return {t[0].lower(): t[1].upper() for t in data_frame.dtypes}


# Rename column names to standardized format by removing spaces and special characters
def normalize_column_name(string: str) -> str:  # pylint: disable=R1710
    if string:
        string = re.sub(r"[^0-9a-zA-Z`!@#$%^&*()_+~\-\=\[\]\{\}:\"\;'\<\>\?,.\\\/]+", "_", string.lower())
        if string and string[0] == "_":
            string = string[1:]
        if string and string[-1] == "_":
            string = string[:-1]
        return string


def normalize_delta_df(layer_df: DataFrame):
    layer_schema = get_schema_from_df(layer_df)
    for layer_column, _ in layer_schema.items():
        normalized_landing_column = normalize_column_name(layer_column)
        layer_df = layer_df.withColumnRenamed(
            layer_column, normalized_landing_column)
    row_count = layer_df.count()
    print(row_count)
    return layer_df

# COMMAND ----------

# This function create 1D list by using split function
def create_list_level_1(string):
    return string.split(",")


# This function creates the 2D list by using ",," as outer list and "," for creating the list in bigger list
def create_list_level_2(string):
    ans = []
    l1 = string.split(",,")
    for i in l1:
        l2 = i.split(",")
        ans.append(l2)
    return ans


# This function is used to split between the cases on which the validation is performed.
def create_list_level_3(string):
    return string.split(",,,")

# COMMAND ----------

def read_filtered_delta_files(file_path,max_date,water_column):
    data_frame = spark.read.format("delta").load(file_path)
    data_frame = data_frame.filter(col(water_column) > max_date)
    return data_frame

def append_data_to_layer(data_frame: DataFrame, location: str):
    data_frame.write.format("delta").option("delta.columnMapping.mode", "name").mode("append").save(location)

    
def upsert_data_to_layer(data_frame: DataFrame, location: str, primary_keys: list):
    # Create a DeltaTable object based on the location of the Delta table
    delta_table = DeltaTable.forPath(spark, location)
    
    update_columns = [col for col in data_frame.columns if col not in primary_keys]

    
    # Merge the incoming data with the existing data in the Delta table based on the primary keys
    merge_condition = " AND ".join([f"existing.{pk} = updates.{pk}" for pk in primary_keys])
    update_dict = {col: f"updates.{col}" for col in update_columns}
    delta_table.alias("existing").merge(
        data_frame.alias("updates"),
        merge_condition
    ).whenMatchedUpdate(set=update_dict).whenNotMatchedInsertAll().execute()

# COMMAND ----------

# DBTITLE 1,Function to return the threshold success status based upon previous and current results.
def threshold(previous_count, current_count, pass_count, fail_count):
    calculated_threshold = int(
        ((current_count - previous_count) / previous_count) * 100)
    if calculated_threshold > 0 and calculated_threshold > upper_accepted_threshold:
        return pass_count, fail_count+1, "Fail"
    elif calculated_threshold < 0 and abs(calculated_threshold) > lower_accepted_threshold:
        return pass_count, fail_count+1, "Fail"
    else:
        return pass_count+1, fail_count, "Pass"


def threshold_kpi_check(prev_result, curr_result):
    calculated_threshold = int(((curr_result - prev_result) / prev_result) * 100)
    if calculated_threshold > 0 and calculated_threshold > upper_accepted_threshold:
        return  "Fail"
    elif calculated_threshold < 0 and abs(calculated_threshold) > lower_accepted_threshold:
        return  "Fail"
    else:
        return "Pass"

# COMMAND ----------

# DBTITLE 1,Functions to return the data for dumping into the audit DB.
# Returns the data to append into the audit db for RowCount check.
def get_row_count_audit_db_data(job_id, task_id, pipeline_name, stage_name, validation, row_count_table_name, row_count_check_count, status, current_user, my_date_time, is_stage_dependency):
    row_count_audit_db = []
    row_count_audit_db.append(int(job_id))
    row_count_audit_db.append(int(task_id))
    row_count_audit_db.append(pipeline_name)
    row_count_audit_db.append(stage_name)
    row_count_audit_db.append(validation)
    row_count_audit_db.extend(["NULL"]*2)
    row_count_audit_db.append(row_count_table_name)
    row_count_audit_db.append("NULL")
    row_count_audit_db.append(row_count_check_count)
    row_count_audit_db.extend(["NULL"]*9)
    row_count_audit_db.append(status)
    row_count_audit_db.append(current_user)
    row_count_audit_db.append(my_date_time)
    row_count_audit_db.append(is_stage_dependency)
    return row_count_audit_db


# Returns the data to append into the audit db for SourceDestinationRowCountCheck.
def get_source_destination_audit_db_data(job_id, task_id, pipeline_name, stage_name, validation, source_layer, destination_layer, agg_function, source_table_name, destination_table_name, source_count, destination_count, status, current_user, my_date_time, is_stage_dependency):
    src_dest_row_count_audit_db = []
    src_dest_row_count_audit_db.append(int(job_id))
    src_dest_row_count_audit_db.append(int(task_id))
    src_dest_row_count_audit_db.append(pipeline_name)
    src_dest_row_count_audit_db.append(stage_name)
    src_dest_row_count_audit_db.append(validation)
    src_dest_row_count_audit_db.append(source_layer)
    src_dest_row_count_audit_db.append(destination_layer)
    src_dest_row_count_audit_db.append("NULL")
    src_dest_row_count_audit_db.append(agg_function)
    src_dest_row_count_audit_db.append("NULL")
    src_dest_row_count_audit_db.append(source_table_name)
    src_dest_row_count_audit_db.append(destination_table_name)
    src_dest_row_count_audit_db.append(source_count)
    src_dest_row_count_audit_db.append(destination_count)
    src_dest_row_count_audit_db.extend(["NULL"]*5)
    src_dest_row_count_audit_db.append(status)
    src_dest_row_count_audit_db.append(current_user)
    src_dest_row_count_audit_db.append(my_date_time)
    src_dest_row_count_audit_db.append(is_stage_dependency)
    return src_dest_row_count_audit_db


# Returns the data to append into the audit db for unique check.
def get_audit_db_data_unique(job_id, task_id, pipeline_name, stage_name, validation_items, table_name, column_name, status,  current_user, my_date_time):
    unique_audit_db = []
    unique_audit_db.append(int(job_id))
    unique_audit_db.append(int(task_id))
    unique_audit_db.append(pipeline_name)
    unique_audit_db.append(stage_name)
    unique_audit_db.append(validation_items)
    unique_audit_db.extend(["NULL"]*2)
    unique_audit_db.append(table_name)
    unique_audit_db.extend(["NULL"]*6)
    unique_audit_db.append(column_name)
    unique_audit_db.extend(["NULL"]*4)
    unique_audit_db.append(status)
    unique_audit_db.append(current_user)
    unique_audit_db.append(my_date_time)
    unique_audit_db.append(0)
    return unique_audit_db


# Returns the data to append into the audit db for unexpected check.
def get_audit_db_data_unexpected(job_id, task_id, pipeline_name, stage_name, validation_items, table_name, column_name, unexpected_value, status,  current_user, my_date_time):
    unexpected_audit_db = []
    unexpected_audit_db.append(int(job_id))
    unexpected_audit_db.append(int(task_id))
    unexpected_audit_db.append(pipeline_name)
    unexpected_audit_db.append(stage_name)
    unexpected_audit_db.append(validation_items)
    unexpected_audit_db.extend(["NULL"]*2)
    unexpected_audit_db.append(table_name)
    unexpected_audit_db.extend(["NULL"]*6)
    unexpected_audit_db.append(column_name)
    unexpected_audit_db.append(unexpected_value)
    unexpected_audit_db.extend(["NULL"]*3)
    unexpected_audit_db.append(status)
    unexpected_audit_db.append(current_user)
    unexpected_audit_db.append(my_date_time)
    unexpected_audit_db.append(0)
    return unexpected_audit_db


# Returns the data to append into the audit db for null check.
def get_audit_db_data_null(job_id, task_id, pipeline_name, stage_name, validation_items, table_name, column_name, status,  current_user, my_date_time):
    null_audit_db = []
    null_audit_db.append(int(job_id))
    null_audit_db.append(int(task_id))
    null_audit_db.append(pipeline_name)
    null_audit_db.append(stage_name)
    null_audit_db.append(validation_items)
    null_audit_db.extend(["NULL"]*2)
    null_audit_db.append(table_name)
    null_audit_db.extend(["NULL"]*6)
    null_audit_db.append(column_name)
    null_audit_db.extend(["NULL"]*4)
    null_audit_db.append(status)
    null_audit_db.append(current_user)
    null_audit_db.append(my_date_time)
    null_audit_db.append(0)
    return null_audit_db


# Returns the data to append into the audit db for pre post runs check.
def get_pre_post_runs_audit_db_data(job_id, task_id, pipeline_name, stage_name, validation, layer, table_name, agg_function, previous_count, current_count, status, current_user, my_date_time, stage_depend):
    pre_post_audit_db = []
    pre_post_audit_db.append(int(job_id))
    pre_post_audit_db.append(int(task_id))
    pre_post_audit_db.append(pipeline_name)
    pre_post_audit_db.append(stage_name)
    pre_post_audit_db.append(validation)
    pre_post_audit_db.append("NULL")
    pre_post_audit_db.append(layer)
    pre_post_audit_db.append(table_name)
    pre_post_audit_db.append(agg_function)
    pre_post_audit_db.extend(["NULL"]*8)
    pre_post_audit_db.append(previous_count)
    pre_post_audit_db.append(current_count)
    pre_post_audit_db.append(status)
    pre_post_audit_db.append(current_user)
    pre_post_audit_db.append(my_date_time)
    pre_post_audit_db.append(stage_depend)
    return pre_post_audit_db


# Returns the data to append into the audit db for KPI result check
def get_kpi_result_audit_db_data(job_id, task_id, pipeline_name, stage_name, validation, table_name, agg_function, kpi_result, current_user, my_date_time, stage_depend):
    kpi_result_audit_db = []
    kpi_result_audit_db.append(int(job_id))
    kpi_result_audit_db.append(int(task_id))
    kpi_result_audit_db.append(pipeline_name)
    kpi_result_audit_db.append(stage_name)
    kpi_result_audit_db.append(validation)
    kpi_result_audit_db.extend(["NULL"]*2)
    kpi_result_audit_db.append(table_name)
    kpi_result_audit_db.append(agg_function)
    kpi_result_audit_db.extend(["NULL"]*7)
    kpi_result_audit_db.append(kpi_result)
    kpi_result_audit_db.extend(["NULL"]*3)
    kpi_result_audit_db.append(current_user)
    kpi_result_audit_db.append(my_date_time)
    kpi_result_audit_db.append(stage_depend)
    return kpi_result_audit_db

# COMMAND ----------

# DBTITLE 1,Functions to Read and write to audit DB
context = adal.AuthenticationContext(authority)
token_audit_db = context.acquire_token_with_client_credentials(resource_app_id_url_audit_db, client_id, client_secret)
access_token_audit_db = token_audit_db["accessToken"]


# Appends the data to audit DB
def append_data_to_audit_db(df: DataFrame, schema: str, table_name: str):
    df.write.format("jdbc") \
        .option("url", azure_sql_url) \
        .option("dbtable", schema+"."+table_name) \
        .option("databaseName", database_name_audit_db) \
        .option("accessToken", access_token_audit_db) \
        .mode("append") \
        .save()


# Reads the data to audit DB
def read_from_audit_db(schema: str, table_name: str) -> DataFrame:
    return spark.read \
        .format("jdbc") \
        .option("url", azure_sql_url) \
        .option("dbtable", schema+"."+table_name) \
        .option("databaseName", database_name_audit_db) \
        .option("accessToken", access_token_audit_db) \
        .option("encrypt", encrypt_audit_db) \
        .option("hostNameInCertificate", host_name_in_certificate_audit_db) \
        .load()

# COMMAND ----------

# DBTITLE 1,Setting configuration for synapse.
def set_read_write_synapse_config():
    spark_config = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/ABCD/oauth2/token",
        # Defining a separate set of service principal credentials for Azure Synapse Analytics (If not defined, the connector will use the Azure storage account credentials)
        "spark.databricks.sqldw.jdbc.service.principal.client.id": client_id,
        "spark.databricks.sqldw.jdbc.service.principal.client.secret": client_secret
    }
    # Setting configurations
    for key, value in spark_config.items():
        spark.conf.set(key, value)

# COMMAND ----------

# DBTITLE 1,Functions to read, write and append into synapse.
def read_from_synapse(table_name: str, layer: str = gold.lower()) -> DataFrame:
    """
        reads data from synapse
        table_name: str    schema_name.table_name
    """
    set_read_write_synapse_config()
    df = spark.read.format("com.databricks.spark.sqldw") \
    .option("url", jdbc_synapse_url) \
    .option("tempDir", set_temporary_directory_url(layer)) \
    .option("enableServicePrincipalAuth","true") \
    .option("useAzureMSI", "true") \
    .option("dbTable", table_name) \
    .load()
    return df

def write_to_synapse(df: DataFrame, table_name: str, layer: str = gold.lower()):
    """
        takes in Dataframe and table_name and writes data to synapse
        df: Dataframe
        table_name: str    schema_name.table_name
    """
    set_read_write_synapse_config()
    df.write.format("com.databricks.spark.sqldw") \
    .option("forward_spark_azure_storage_credentials", True) \
    .option("enableServicePrincipalAuth","true") \
    .option("url", jdbc_synapse_url) \
    .option("tempDir", set_temporary_directory_url(layer)) \
    .option("dbTable", table_name) \
    .option("tableOptions", "DISTRIBUTION = ROUND_ROBIN, HEAP") \
    .mode("overwrite") \
    .save()
    print(f"Wrote {table_name} to {jdbc_synapse_url}")
    
def write_to_synapse_append(df: DataFrame, table_name: str, layer: str = gold.lower()):
    """
        takes in Dataframe and table_name and appends data to synapse
        df: Dataframe
        table_name: str    schema_name.table_name
    """
    set_read_write_synapse_config()
    df.write.format("com.databricks.spark.sqldw") \
    .option("forward_spark_azure_storage_credentials", True) \
    .option("enableServicePrincipalAuth","true") \
    .option("url", jdbc_synapse_url) \
    .option("tempDir", set_temporary_directory_url(layer)) \
    .option("dbTable", table_name) \
    .mode("append") \
    .save()
    
    print(f"Wrote {table_name} to {jdbc_synapse_url}")
    
def write_to_synapse_truncate_append(df: DataFrame, table_name: str, layer: str = gold.lower()):
    """
        takes in Dataframe and table_name and first truncates table then appends data to synapse
        retaining the schema details of the table
        df: Dataframe
        table_name: str    schema_name.table_name
    """
    connect = call_stored_procedure(f"EXEC {stored_procedure_truncate_table} '{table_name}', 'TRUNCATE TABLE {table_name}'")
    connect.close()
    set_read_write_synapse_config()
    df.write.format("com.databricks.spark.sqldw") \
    .option("forward_spark_azure_storage_credentials", True) \
    .option("enableServicePrincipalAuth","true") \
    .option("url", jdbc_synapse_url) \
    .option("tempDir", set_temporary_directory_url(layer)) \
    .option("dbTable", table_name) \
    .mode("append") \
    .save()
    
    print(f"Wrote {table_name} to {jdbc_synapse_url}")

# COMMAND ----------

# DBTITLE 1,Setting config for SP.
def set_config_for_stored_procedure():
    app = msal.ConfidentialClientApplication(client_id, client_secret, authority)
    token_stored_procedure = app.acquire_token_for_client(scopes="https://database.windows.net/.default")["access_token"]
    properties = spark._sc._gateway.jvm.java.util.Properties()
    properties.setProperty("accessToken", token_stored_procedure)
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    con = driver_manager.getConnection(jdbc_synapse_url, properties)
    return con

# COMMAND ----------

# DBTITLE 1,Calling the SP as mentioned in the passed parameter.
def call_stored_procedure(exec_sp_query):
    connect = set_config_for_stored_procedure()
    exec_statement = connect.prepareCall(exec_sp_query)
    exec_statement.execute()
    return connect

# COMMAND ----------

# DBTITLE 1,Move the specified columns in the data frame to the front
def get_cols_to_front(df, columns_to_front) :
    original = df.columns
    columns_other = list(set(original) - set(columns_to_front))
    columns_other.sort()
    df = df.select(*columns_to_front, *columns_other)
    return df

# COMMAND ----------

# DBTITLE 1,Function to return the result from the synapse based upon the parametrized query
def query_on_synapse(query, layer):
    execute_statement = f"""EXEC {stored_procedure_name} 'With cte as ({query}) SELECT * INTO {table_for_storing_kpi_result} from cte', '{table_for_storing_kpi_result}'"""
    connect = call_stored_procedure(execute_statement)
    return read_from_synapse(f'{table_for_storing_kpi_result}', layer), query

# COMMAND ----------

# DBTITLE 1,Function to return the result from the ADB SQL based upon the parametrized query
def query_on_adb_sql(query):
    return spark.sql(query), query