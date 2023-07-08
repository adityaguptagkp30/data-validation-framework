# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###### Purpose: Configuration for every notebook
# MAGIC
# MAGIC ###### Parameter Info:
# MAGIC
# MAGIC ###### Revision History:
# MAGIC
# MAGIC |Date           |Author             |Description                                                                                        |Execution Time          |
# MAGIC |---------------|:-----------------:|---------------------------------------------------------------------------------------------------|------------------------|
# MAGIC |Feb 23, 2023   |Rohit Singla       |Created this notebook for the configurations which are globally required in other notebooks        |                        |
# MAGIC |May 19, 2023   |Aditya Gupta      |Updated the UAT and PROD environment key vault scope                                               |                        |

# COMMAND ----------

# MAGIC %run ./Config_BaseTable_Mapping

# COMMAND ----------

# MAGIC %run ./Config_Variables

# COMMAND ----------

# MAGIC %md
# MAGIC Config variables initialized at the start of Pipeline execution

# COMMAND ----------

# DBTITLE 1,Function for setting access to connect ADLS.
def set_spark_conf(storage_account):
    if environment == "dev":
        spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", dbutils.secrets.get(scope=f"DEV-Credentials", key=f"{storage_account}-key"))
    elif environment == "uat":
        spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", dbutils.secrets.get(scope=f"UAT-Credentials", key=f"{storage_account}-key"))
    elif environment == "prod":
        spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", dbutils.secrets.get(scope=f"PROD-Credentials", key=f"{storage_account}-key"))

# COMMAND ----------

# DBTITLE 1,Create the URL for the abfss path
def get_storage_uri(environment,stage):
    return 'abfss://' + container[environment][stage] + '@' + storage[environment][stage] + '.dfs.core.windows.net'

# COMMAND ----------

# DBTITLE 1,Returns the destination abfss URL for particular stage name
def set_destination_locations():
    if StageName == Gold_stage:
        return get_storage_uri(environment,'gold')
    elif StageName == Silver_stage:
        return get_storage_uri(environment,'silver')
    elif StageName == Bronze_stage:
        return get_storage_uri(environment,'bronze')

# COMMAND ----------

# DBTITLE 1,Returns the source abfss URL for particular stage name
def set_source_locations():
    if StageName == Gold_stage:
        return get_storage_uri(environment,'silver')
    elif StageName == Silver_stage:
        return get_storage_uri(environment,'bronze')
    elif StageName == Bronze_stage:
        return get_storage_uri(environment,'bronze')

# COMMAND ----------

# DBTITLE 1,Returns the container for particular stage name
def get_container_name():
    if StageName == Gold_stage:
        return container[environment]['gold']
    elif StageName == Silver_stage:
        return container[environment]['silver']
    elif StageName == Bronze_stage:
        return container[environment]['bronze']

# COMMAND ----------

# DBTITLE 1,Returns the storage account for particular stage name
def get_storage_account_name():
    if StageName == Gold_stage or StageName == Synapse_stage:
        return storage[environment]['gold']
    elif StageName == Silver_stage:
        return storage[environment]['silver']
    elif StageName == Bronze_stage:
        return storage[environment]['bronze']

# COMMAND ----------

# DBTITLE 1,Returns the storage account for pre post runs check
def get_storage_account_name_pre_post_runs(layer):
    if layer == gold:
        return storage[environment]['gold']
    elif layer == silver:
        return storage[environment]['silver']
    elif layer == bronze:
        return storage[environment]['bronze']

# COMMAND ----------

# DBTITLE 1,Returns the abfss, container and storage account for pre post runs check.
def get_PrePostRunsRowcountCheck_locations(layer):
    if layer == bronze:
        return get_storage_uri(environment,'bronze'), container[environment]['bronze'], storage[environment]['bronze']
    elif layer == silver:
        return get_storage_uri(environment,'silver'), container[environment]['silver'], storage[environment]['silver']
    elif layer == gold:
        return get_storage_uri(environment,'gold'), container[environment]['gold'], storage[environment]['gold']

# COMMAND ----------

# Returns the source link and the destination link based upon the source layer and destination layer provided.
def get_source_destination_urls(source_layer, destination_layer):
    source_layer = source_layer.lower()
    destination_layer = destination_layer.lower()
    return get_storage_uri(environment, source_layer), get_storage_uri(environment, destination_layer)

# COMMAND ----------

from typing import Optional
def get_adls_table_path(base_table_name: str, layer_name: str, parent_directory: Optional[str] = '', sub_area: Optional[str] = '') -> str:
    layer_name = layer_name.lower()
    base_table_name = base_table_name.upper()
    if layer_name == 'bronze':
        return get_storage_uri(environment, layer_name) + Table_detail[base_table_name][layer_name]
    elif layer_name == 'silver' or layer_name == 'gold':
        if parent_directory == 'core':
            parent_directory = '/Core'
        elif parent_directory == 'derived':
            parent_directory = '/Derived'
        if sub_area != '':
           sub_area = '/' + sub_area
        return get_storage_uri(environment, layer_name) + parent_directory + sub_area + '/' + base_table_name
    
def get_adls_path(table_name: str, layer_name: str) -> str:
    return get_storage_uri(environment, layer_name) + Table_detail[table_name][layer_name]