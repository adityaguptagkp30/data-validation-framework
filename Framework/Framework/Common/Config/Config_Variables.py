# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###### Purpose: Configuration Variables for every notebook
# MAGIC
# MAGIC ###### Parameter Info:
# MAGIC
# MAGIC ###### Revision History:
# MAGIC
# MAGIC |Date           |Author             |Description                                                                                        |Execution Time          |
# MAGIC |---------------|:-----------------:|---------------------------------------------------------------------------------------------------|------------------------|
# MAGIC |Mar 15, 2023   |Rohit Singla       |Created this notebook for the variables which are globally required in other notebooks             |                        |
# MAGIC |May 19, 2023   |Rohit Singla      |Updated the UAT and PROD environment key vault scope                                               |                        |

# COMMAND ----------

# DBTITLE 1,Template of JSON Format.
# This commented code is for referance of the input parameters for data quality notebook.
json_params={
          "NullCheckList": 
          [
      #       {
      #         "Schema": "",
      #         "TableName": "",
      #         "columns_list": []
      #       },
          ],

          "UniqueCheckList": 
          [
      #       {
      #         "Schema": "",
      #         "TableName": "",
      #         "columns_list":[],
      #       },
          ],

          "UnExpectedList":
          [
      #       {
      #         "Schema": "",
      #         "TableName": "",
      #         "column_name": "",
      #         "unexpected_list": []
      #       },
          ],

          "ForeignKeyCheck":
          [
      #       {
      #         "Schema": "default",

      #         "DimTableName": "",
      #         "Dim_column_name": "",

      #         "FactTableName": "",
      #         "Fact_column_name": []
      #       },
          ],

          "SLACheckList": 
          [
      #       {
      #         "Schema": "",
      #         "TableName": "",
      #         "column_name":"",
      #         "SLAThreshold":""
      #       },
          ],
      }

# COMMAND ----------

# DBTITLE 1,JSON for getting the storage name for specified environment.
storage = {
    "dev" : {
        'bronze' : 'storageName',
        'silver' : 'containerName',
        'gold' : 'containerName',
        'configs': 'containerName'
    },
    "uat" : {
        'bronze' : 'containerName',
        'silver' : 'containerName',
        'gold' : 'containerName',
        'configs': 'containerName'
    },
    "prod" : {
        'bronze' : 'containerName',
        'silver' : 'containerName',
        'gold' : 'containerName',
        'configs': 'containerName'
    }
}

# COMMAND ----------

# DBTITLE 1,JSON for getting the Container name for specified environment.
container = {
    "dev" : {
        'bronze' : 'containerName',
        'silver' : 'containerName',
        'gold' : 'containerName',
        'configs': 'containerName'
    },
    "uat" : {
        'bronze' : 'containerName',
        'silver' : 'containerName',
        'gold' : 'containerName',
        'configs': 'containerName'
    },
    "prod" : {
        'bronze' : 'containerName',
        'silver' : 'containerName',
        'gold' : 'containerName',
        'configs': 'containerName'
    }
}

# COMMAND ----------

# DBTITLE 1,Setting the location type checks into variables.
ADLS = "ADLS"
ADB = "ADB_SQL"
SYNAPSE = "Synapse"

# COMMAND ----------

# DBTITLE 1,Information related to the Stored Procedure in Synapse.
table_for_storing_kpi_result = 'SP_NAME'
stored_procedure_name = 'SP_NAME'
stored_procedure_truncate_table = "SP_NAME"

# COMMAND ----------

# DBTITLE 1,Location for storing the current data into DBFS 
output_result_location  = "/FileStore/Data_Foundation/Result/"

# COMMAND ----------

# DBTITLE 1,Parameters for reading the csv file.
input_csv_path = '/configs/DF_Validation_TestCases.csv'
delimeter = ","

# COMMAND ----------

# DBTITLE 1,Prefix for sending the mail.
project_name = "Data Foundation"

# COMMAND ----------

# DBTITLE 1,Checks config variables as per ValidationConfiguration excel file
Row_Count = 'RowCount'
Source_DestinationRowcountResult = 'SourceDestinationRowcountResult'
UnExpected_List = 'UnExpectedList'
Unique_Check= 'UniqueCheck'
Null_Check='NullCheck'
ForeignKey_Check='ForeignKeyCheck'
PrePostRowcount_Check='PrePostRowcountCheck'
KPIResult_Check = "KPIResultCheck"

# COMMAND ----------

# DBTITLE 1,Stage names as per validation excel.
PrePostRunsRowcountCheck_Stage='Pre and post runs rowcount check'
Synapse_stage = 'Synapse data check'
Bronze_stage = "Bronze data check"
Silver_stage = "Silver data check"
Gold_stage = "Gold data check"
KPI_stage = "KPI data check"

# COMMAND ----------

# DBTITLE 1,Layer name as per validation excel
bronze = "Bronze"
silver = "Silver"
gold = "Gold"

# COMMAND ----------

# DBTITLE 1,Upper and Lower Limit of Threshold.
lower_accepted_threshold = 5
upper_accepted_threshold = 20

# COMMAND ----------

if environment == 'dev':
    client_id = dbutils.secrets.get(scope="dev-scope", key="dev-spn-clientid")
    client_secret = dbutils.secrets.get(scope="dev-scope", key="dev-spn-secret")
    tenant_id = dbutils.secrets.get(scope="dev-scope", key="dev-spn-tenantid")
    authority = f"https://login.windows.net/{tenant_id}"
    port = "1433"

elif environment == 'uat':
    client_id = dbutils.secrets.get(scope="preprod-scope", key="preprod-spn-clientid")
    client_secret = dbutils.secrets.get(scope="preprod-scope", key="preprod-spn-secret")
    tenant_id = dbutils.secrets.get(scope="preprod-scope", key="preprod-spn-tenantid")
    authority = f"https://login.windows.net/{tenant_id}"
    port = "1433"

elif environment == 'prod':
    client_id = dbutils.secrets.get(scope="prod-scope", key="pbna-dfn-prod-spn-clientid")
    client_secret = dbutils.secrets.get(scope="prod-scope", key="prod-spn-secret")
    tenant_id = dbutils.secrets.get(scope="prod-scope", key="prod-spn-tenantid")
    authority = f"https://login.windows.net/{tenant_id}"
    port = "1433"

# COMMAND ----------

# DBTITLE 1,Configuration variables for the audit DB.
resource_app_id_url_audit_db = "https://database.windows.net"
encrypt_audit_db = "true"
host_name_in_certificate_audit_db="*.database.windows.net"

if environment == 'dev':
    server_name_audit_db = "dev.database.windows.net"
    database_name_audit_db = "dev_dfn"
    azure_sql_url = f"jdbc:sqlserver://{server_name_audit_db}"
    
elif environment == 'uat':
    server_name_audit_db = "prod.database.windows.net"
    database_name_audit_db = "preprod_dfn"
    azure_sql_url = f"jdbc:sqlserver://{server_name_audit_db}"
    
elif environment == 'prod':
    server_name_audit_db = "prod.database.windows.net"
    database_name_audit_db = "prod_dfn"
    azure_sql_url = f"jdbc:sqlserver://{server_name_audit_db}"

# COMMAND ----------

# DBTITLE 1,Configuration variables for the Synapse and for Call procedure.
# This function sets the temporary directory link based upon the layer which is used for ADLS.
dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"
if environment == 'dev':
    database_name_synapse = "devpool"
    server_name_synapse = "synapseDev.sql.azuresynapse.net"
    jdbc_synapse_url = f"jdbc:sqlserver://{server_name_synapse}:{port};database={database_name_synapse}"
    
elif environment == 'uat':
    database_name_synapse = "uatpool"
    server_name_synapse = "synapseUat.sql.azuresynapse.net"
    jdbc_synapse_url = f"jdbc:sqlserver://{server_name_synapse}:{port};database={database_name_synapse}"
    
elif environment == 'prod':
    database_name_synapse = "prodpool"
    server_name_synapse = "synapseProd.sql.azuresynapse.net"
    jdbc_synapse_url = f"jdbc:sqlserver://{server_name_synapse}:{port};database={database_name_synapse}"

def set_temporary_directory_url(layer):
    return f"abfss://{container[environment][layer]}@{storage[environment][layer]}.dfs.core.windows.net/Synapse"

# COMMAND ----------

# DBTITLE 1,Schema and table name which stores the data of the validation framework and will contain the history of 35 days.
schema_historical = "PBNA_AUDIT"
tableName_historical = "VLDTN_HSTRCL_RSLT"
schema_notification = "PBNA_MTADTA"
tablename_notification = "CNFGRTN_MPPNG"

# COMMAND ----------

# DBTITLE 1,List of column names as same as in audit DB.
column_audit_db = ["JOB_ID", "TSK_ID", "PPLN_NM", "STG_NM", "VLDTN_CHCK_TYP", "SRC_LAYR", "DEST_LAYR","TBL_NM", "AGGR_FNCTN", "ROW_CNT", "SRC_TBL_NM", "DEST_TBL_NM", "SRC_RSLT", "DEST_RSLT", "COL_NM", "UNXPCTD_VAL", "KPI_RSLT", "POST_RUN_RSLT", "PRE_RUN_RSLT", "VLDTN_RSLT", "CRTD_BY", "CRTD_TM", "ON_STG_DPND"]