# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ###### Purpose: Child Notbook of the Master Notebook for Validation
# MAGIC
# MAGIC ###### Parameter Info:
# MAGIC
# MAGIC ###### Revision History:
# MAGIC
# MAGIC |Date           |Author             |Description                                                                                        |Execution Time          |
# MAGIC |---------------|:-----------------:|---------------------------------------------------------------------------------------------------|------------------------|
# MAGIC |Feb 28, 2023   |Rohit Singla       |Created Row Count validation notebook for validation framework                                     |00:01:02                |

# COMMAND ----------

environment = getArgument("Environment", "")

# COMMAND ----------

# MAGIC %run ../../Common/Utilities/Utilities

# COMMAND ----------

# DBTITLE 1,Passed Parameters.
JobID = getArgument("JobID", "")
TaskID = getArgument("TaskID", "")

stage_depend_string = getArgument("StageDepend", "")
stage_depend_list = create_list_level_2(stage_depend_string)

Location_Type_string = getArgument("LocationType", "")
Location_Type_list = create_list_level_2(Location_Type_string)

destination_storage_account_url = getArgument("DestinationTableLocationURL", "")
source_storage_account_url = getArgument("SourceTableLocationURL", "")
rowcount_storage_account_url = getArgument("RowCountTableLocationURL", "")

Result_store_location = getArgument("OutputStoreLocation","")

ValidationTypeString = getArgument("ValidationType","")
ValidationType = create_list_level_1(ValidationTypeString)

RowCountTableNameString = getArgument("RowCountTableName", "")
Row_Count_Table_List = create_list_level_1(RowCountTableNameString)

SourceRowCountTableNameString = getArgument("SourceRowCountTableName", "")
Source_Row_Count_Table_List = create_list_level_1(SourceRowCountTableNameString)

DestinationRowCountTableNameString = getArgument("DestinationRowCountTableName", "")
Destination_Row_Count_Table_List = create_list_level_1(DestinationRowCountTableNameString)

dir_for_row_count_string = getArgument("TableLocationRowCount", "")
dir_for_row_count_list = create_list_level_1(dir_for_row_count_string)

dir_for_source_row_count_string = getArgument("TableLocationSourceRowCount", "")
dir_for_source_row_count_list = create_list_level_1(dir_for_source_row_count_string)

dir_for_destination_row_count_string = getArgument("TableLocationDestinationRowCount", "")
dir_for_destination_row_count_list = create_list_level_1(dir_for_destination_row_count_string)

StageName = getArgument("StageName","")
PipelineName = getArgument("PipelineName","")

# COMMAND ----------

# Sets the configuration for Current stage name
set_spark_conf(get_storage_account_name())

# COMMAND ----------

# Creates the row count data by returning the list and other details.
def get_data_row_count(row_count_table_name, fail_count, pass_count, status, row_ind, col_ind):
    row_count_list = []
    row_count_list.append(row_count_table_name)
    my_date_time = datetime.datetime.now()
    row_count_list.append(my_date_time.strftime("%m-%d-%Y %H:%M"))
    if Location_Type_list[row_ind][col_ind] == ADLS:
        Row_Count_Check_table = read_delta_files(rowcount_storage_account_url + dir_for_row_count_list[col_ind])
    elif Location_Type_list[row_ind][col_ind] == ADB:
        Row_Count_Check_table = get_data_from_tables(dir_for_row_count_list[col_ind])
    elif Location_Type_list[row_ind][col_ind] == SYNAPSE:
        Row_Count_Check_table = read_from_synapse(dir_for_row_count_list[col_ind])
    Row_Count_Check_Count = Row_Count_Check_table.count()
    row_count_list.append("{:,}".format(Row_Count_Check_Count))
    if Row_Count_Check_Count == 0:
        status = "Fail"
        fail_count = fail_count+1
    else:
        status = "Pass"
        pass_count = pass_count+1
    row_count_list.append(status)
    return row_count_list, fail_count, pass_count, Row_Count_Check_Count, status

# COMMAND ----------

# Creates the source destination row count data by returning the list and other details.
def get_data_source_destination_row_count(source_table_name, destination_table_name, fail_count, pass_count, status, row_ind, col_ind):
    source_destination_list = []
    source_destination_list.append(source_table_name)
    source_destination_list.append(destination_table_name)
    my_date_time = datetime.datetime.now()
    source_destination_list.append(my_date_time.strftime("%m-%d-%Y %H:%M"))
    if Location_Type_list[row_ind][col_ind] == ADLS:
        Source_Parquet_table = read_delta_files(source_storage_account_url + dir_for_source_row_count_list[col_ind])
        Destination_Parquet_table = read_delta_files(destination_storage_account_url+dir_for_destination_row_count_list[col_ind])
    elif Location_Type_list[row_ind][col_ind] == ADB:
        Source_Parquet_table = get_data_from_tables(dir_for_source_row_count_list[col_ind])
        Destination_Parquet_table = get_data_from_tables(dir_for_destination_row_count_list[col_ind])
    elif Location_Type_list[row_ind][col_ind] == SYNAPSE:
        Source_Parquet_table = read_from_synapse(dir_for_source_row_count_list[col_ind])
        Destination_Parquet_table = read_from_synapse(dir_for_destination_row_count_list[col_ind])
    
    Source_count = Source_Parquet_table.count()
    Destination_count = Destination_Parquet_table.count()
    source_destination_list.append("{:,}".format(Source_count))
    source_destination_list.append("{:,}".format(Destination_count))
    pass_count, fail_count, status = threshold(Source_count, Destination_count, pass_count, fail_count)
    source_destination_list.append(status)
    return source_destination_list, fail_count, pass_count, Source_count, Destination_count, status

# COMMAND ----------

# DBTITLE 1,5. Calculating the Row Count and Return the Data Frame.
Summary_columns = ["Validation_Category", "Total_Tables", "Passed", "Failed" ]
Data = []
for index, validation in enumerate(ValidationType):
    status = ''
    pass_count = 0
    fail_count = 0
    Table_count = 0
    Row_count_table = []
    auditDB_data = []
    if validation == Row_Count:
        Columns = ["Table_Name", "Validated_On", "Row_Count", "Validation_Result"]
        for ind, row_count_table_name in enumerate(Row_Count_Table_List):
            Table_count = Table_count+1
            row_count_list, fail_count, pass_count, rowcount, status= get_data_row_count(row_count_table_name, fail_count, pass_count, status, index, ind)
            Row_count_table.append(row_count_list)
            auditDB_data.append(
                get_row_count_audit_db_data(
                    JobID\
                    , TaskID\
                    , PipelineName\
                    , StageName\
                    , validation\
                    , row_count_table_name\
                    , str(rowcount)\
                    , status\
                    , current_user\
                    , datetime.datetime.now()\
                    , int(stage_depend_list[index][ind])
                )
            )
        
        Data.append(["Row Count", Table_count, pass_count, fail_count])
        Dataframe = spark.createDataFrame(Row_count_table, Columns)
        overwrite_data_to_layer(Dataframe, Result_store_location + StageName + "/" + "Row Count")
        df = spark.createDataFrame(auditDB_data, column_audit_db)
        append_data_to_audit_db(match_the_schema_with_audit_db(df), schema_historical, tableName_historical)
            
    else:
        Columns = ["Source_Table_Name", "Destination_Table_Name", "Validated_On", "Source_Row_Count", "Destination_Row_Count", "Validation_Result"]
        for ind, (source_table_name, destination_table_name) in enumerate(zip(Source_Row_Count_Table_List, Destination_Row_Count_Table_List)):
            Table_count = Table_count+1
            source_destination_list, fail_count, pass_count, sourcerowcount, desrowcount, status = get_data_source_destination_row_count(source_table_name, destination_table_name, fail_count, pass_count, status, index, ind)
            Row_count_table.append(source_destination_list)
            auditDB_data.append(
                get_source_destination_audit_db_data(
                    JobID\
                    , TaskID\
                    , PipelineName\
                    , StageName\
                    , validation\
                    , "null"\
                    , "null"\
                    , "null"\
                    , source_table_name\
                    , destination_table_name\
                    , str(sourcerowcount)\
                    , str(desrowcount)\
                    , status\
                    , current_user\
                    , datetime.datetime.now()\
                    , int(stage_depend_list[index][ind])
                )
            )
        Data.append(["Source Destination Row Count", Table_count, pass_count, fail_count])
        Dataframe = spark.createDataFrame(Row_count_table, Columns)
        overwrite_data_to_layer(Dataframe, Result_store_location + StageName + "/" + "Source Destination Row Count")
        df = spark.createDataFrame(auditDB_data, column_audit_db)
        append_data_to_audit_db(match_the_schema_with_audit_db(df), schema_historical, tableName_historical)

Validation_Summary_df = spark.createDataFrame(Data, Summary_columns)
overwrite_data_to_layer(Validation_Summary_df, Result_store_location + StageName + "/" +"Summary Table")