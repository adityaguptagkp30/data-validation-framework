# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###### Purpose: Child Notbook of the Master Notebook for Validation
# MAGIC
# MAGIC ###### Parameter Info:
# MAGIC
# MAGIC ###### Revision History:
# MAGIC
# MAGIC |Date           |Author             |Description                                                                                        |Execution Time          |
# MAGIC |---------------|:-----------------:|---------------------------------------------------------------------------------------------------|------------------------|
# MAGIC |Apr 03, 2023   |Rohit Singla       |Returns the validation status of the KPI on the synapse data.                                      |                        |

# COMMAND ----------

environment = getArgument("Environment", "")

# COMMAND ----------

# MAGIC %run ../../Common/Utilities/Utilities

# COMMAND ----------

# DBTITLE 1,Parameters accepted from the master notebook.
JobID = getArgument("JobID", "")
TaskID = getArgument("TaskID", "")
PipelineName = getArgument("PipelineName", "")

unique_id_string = getArgument("UniqueID", "")
unique_id_list = create_list_level_1(unique_id_string)

destination_location_type_string = getArgument('DestinationLocationType', "")
destination_location_type_list = create_list_level_1(destination_location_type_string)

source_location_type_string = getArgument('SourceLocationType', "")
source_location_type_list = create_list_level_1(source_location_type_string)

kpi_name_string = getArgument("KPIName", "")
kpi_name_list = create_list_level_1(kpi_name_string)

dest_table_location_string = getArgument("DestinationTableLocation","")
dest_table_location_list = create_list_level_1(dest_table_location_string)

source_table_location_string = getArgument("SourceTableLocation","")
source_table_location_list = create_list_level_1(source_table_location_string)

destination_layer_string = getArgument("DestinationLayer","")
destination_layer_list = create_list_level_1(destination_layer_string)

source_layer_string = getArgument("SourceLayer","")
source_layer_list = create_list_level_1(source_layer_string)

source_query_string = getArgument("SourceQuery", "")
source_query_list = create_list_level_3(source_query_string)

destination_query_string = getArgument("DestinationQuery", "")
destination_query_list = create_list_level_3(destination_query_string)

aggregate_function_string = getArgument("AggregateFunction","")
aggregate_function_list = create_list_level_1(aggregate_function_string)

validation_check_string = getArgument("ValidationCheck","")
validation_check_list = create_list_level_1(validation_check_string)

destination_table_name_string = getArgument("DestinationTableName","")
destination_table_name_list = create_list_level_1(destination_table_name_string)

source_table_name_string = getArgument("SourceTableName","")
source_table_name_list = create_list_level_1(source_table_name_string)

column_name_string = getArgument("ColumnName","")
column_name_list = create_list_level_1(column_name_string)

stage_depend_string = getArgument("StageDepend", "")
stage_depend_list = create_list_level_1(stage_depend_string)

result_store_location = getArgument("OutputStoreLocation", "")

StageName = KPI_stage

# COMMAND ----------

# DBTITLE 1,Function to return the result from the ADLS.
# Index and table location is provided as the input. Output of the query and the query itself.
def query_on_adls(index, table_location):
    query = f"""SELECT {aggregate_function_list[index]}({column_name_list[index]}) AS {aggregate_function_list[index]} FROM DELTA.`{table_location}`"""
    return spark.sql(query), query

# COMMAND ----------

# DBTITLE 1,Compare the result from the last run of pipeline with the current.
# Accepts the xml file location, uniqueID and current result as an input and returns the previous result as well as its status. 
def kpi_result_compare_to_stored_xml(xml_location, unique_id, curr_result):
    tree = ET.parse(xml_location)
    root_tag = tree.getroot()
    for row in root_tag:
        if row[0].text == unique_id:
            Status = threshold_kpi_check(int(row[1].text), curr_result)
            return int(row[1].text), Status
    if curr_result == 0:
        return 0, "Fail"
    return 0, "Pass"

# COMMAND ----------

# DBTITLE 1,Creates the string for xml file, Data for audit DB and output mail table for Pre-Post pipeline run check.
# Accepts the current result as dataframe, index to get the values from the parameters rather than passing all those variables in the function, previous result, status, query, current xml updated string.
# Returns the list of the values which are stored in the DBFS.
def get_kpi_data_pre_post_run(output_data_from_query_df, dest_loc_type_index, prev_result, status, query, output):
    kpi_summary_info_list.append("Pre-Post Run")
    output += f"""<Row><UniqueID>{unique_id_list[dest_loc_type_index]}</UniqueID><Result>{output_data_from_query_df.collect()[0][0]}</Result></Row>"""
    if os.path.exists("/dbfs"+result_store_location+'/'+StageName+"/"+"PreCheck.xml"):
        prev_result, status = kpi_result_compare_to_stored_xml(
            "/dbfs"+result_store_location+'/'+StageName+"/"+"PreCheck.xml"\
            ,unique_id_list[dest_loc_type_index]\
            ,output_data_from_query_df.collect()[0][0]
        )
    else:
        if output_data_from_query_df.collect()[0][0] == 0:
            status = "Fail"
    
    data_auditDB.append(
        get_pre_post_runs_audit_db_data(
            JobID\
            ,TaskID\
            ,PipelineName\
            ,StageName\
            ,validation_check_list[dest_loc_type_index]\
            ,destination_layer_list[dest_loc_type_index]\
            ,destination_table_name_list[dest_loc_type_index]\
            ,aggregate_function_list[dest_loc_type_index]\
            ,str(prev_result)\
            ,str(output_data_from_query_df.collect()[0][0])\
            ,status\
            ,current_user\
            ,my_date_time\
            ,int(stage_depend_list[dest_loc_type_index])
        )
    )

    return [
        kpi_name_list[dest_loc_type_index]\
        ,destination_table_name_list[dest_loc_type_index]\
        ,destination_layer_list[dest_loc_type_index]\
        ,query\
        ,aggregate_function_list[dest_loc_type_index]\
        ,my_date_time.strftime("%m-%d-%Y %H:%M")\
        ,"{:,}".format(prev_result)\
        ,"{:,}".format(output_data_from_query_df.collect()[0][0])\
        ,status
    ]

# COMMAND ----------

# DBTITLE 1,Creates Data for audit DB and output mail table for source destination check.
# Accepts the source and destination output as dataframe index, destination query and source query.
# Returns the list of the values which are stored in the DBFS.
def get_kpi_data_source_destination(source_output_data_from_query_df, dest_output_data_from_query_df, dest_loc_type_index, query_dest, query_source):
    status = threshold_kpi_check(int(source_output_data_from_query_df.collect()[0][0]), int(dest_output_data_from_query_df.collect()[0][0]))
    kpi_summary_info_list.append("Source Destination")
    data_auditDB.append(
        get_source_destination_audit_db_data(
            JobID\
            ,TaskID\
            ,PipelineName\
            ,StageName\
            ,validation_check_list[dest_loc_type_index]\
            ,source_layer_list[dest_loc_type_index]\
            ,destination_layer_list[dest_loc_type_index]\
            ,aggregate_function_list[dest_loc_type_index]\
            ,source_table_name_list[dest_loc_type_index]\
            ,destination_table_name_list[dest_loc_type_index]\
            ,str(source_output_data_from_query_df.collect()[0][0])\
            ,str(dest_output_data_from_query_df.collect()[0][0])\
            ,status\
            ,current_user\
            ,datetime.datetime.now()\
            ,int(stage_depend_list[dest_loc_type_index])
        )
    )

    return [
        kpi_name_list[dest_loc_type_index]\
        ,source_table_name_list[dest_loc_type_index]\
        ,destination_table_name_list[dest_loc_type_index]\
        ,source_layer_list[dest_loc_type_index]\
        ,destination_layer_list[dest_loc_type_index]\
        ,query_source\
        ,query_dest\
        ,aggregate_function_list[dest_loc_type_index]\
        ,my_date_time.strftime("%m-%d-%Y %H:%M")\
        ,"{:,}".format(source_output_data_from_query_df.collect()[0][0])\
        ,"{:,}".format(dest_output_data_from_query_df.collect()[0][0])\
        ,status
    ]

# COMMAND ----------

# DBTITLE 1,Creates Data for audit DB and output mail table for KPI result check.
# Accepts the index, output of the KPI result as dataframe, and Query.
# Returns the list of the values which are stored in the DBFS.
def get_kpi_data_kpi_result(dest_loc_type_index, output_data_from_query_df, query):
    kpi_summary_info_list.append("KPI")
    data_auditDB.append(
        get_kpi_result_audit_db_data(
            JobID\
            ,TaskID\
            ,PipelineName\
            ,StageName\
            ,validation_check_list[dest_loc_type_index]\
            ,destination_table_name_list[dest_loc_type_index]\
            ,aggregate_function_list[dest_loc_type_index]\
            ,str(output_data_from_query_df.collect()[0][0])\
            ,current_user\
            ,datetime.datetime.now()\
            ,int(stage_depend_list[dest_loc_type_index])
        )
    )

    return [
        kpi_name_list[dest_loc_type_index]\
        ,destination_table_name_list[dest_loc_type_index]\
        ,query\
        ,aggregate_function_list[dest_loc_type_index]\
        ,my_date_time.strftime("%m-%d-%Y %H:%M")\
        ,"{:,}".format(output_data_from_query_df.collect()[0][0])
    ]

# COMMAND ----------

# DBTITLE 1,Variables used and the column list for the tables.
summary_columns = ["Validation_Checks"]
source_destination_column = ["KPI_Name", "Source_Table_Name", "Destination_Table_Name", "Source_Layer", "Destination_Layer", "Source_Query", "Destination_Query", "Aggregate_Function", "Validated_On", "Source_Result", "Destination_Result", "Validation_Result"]
pre_post_column = ["KPI_Name", "Table_Name", "Layer", "Query", "Aggregate_Function", "Validated_On", "Previous_Result", "Current_Result", "Validation_Result"]
kpi_result_column = ["KPI_Name", "Table_Name", "Query", "Aggregate_Function", "Validated_On", "KPI_Result"]
summary_data = []
kpi_summary_info_list = []
prepostdata = []
sourcedestinationdata = []
kpiresultdata = []
data_auditDB = []

# COMMAND ----------

# DBTITLE 1,Main driver code for call the above functions.
output = "<Table>"
for dest_loc_type_index, dest_loc_type in enumerate(destination_location_type_list):
    my_date_time = datetime.datetime.now()
    prev_result = 0
    status = "Pass"
    query = ''
    if dest_loc_type == ADLS:
        if validation_check_list[dest_loc_type_index] == PrePostRowcount_Check:
            output_data_from_query_df, query = query_on_adls(dest_loc_type_index, get_storage_uri(environment, destination_layer_list[dest_loc_type_index].lower()) + dest_table_location_list[dest_loc_type_index])
            prepostdata.append(get_kpi_data_pre_post_run(output_data_from_query_df, dest_loc_type_index, prev_result, status, query, output))
            
        elif validation_check_list[dest_loc_type_index] == Source_DestinationRowcountResult:
            source_url, destination_url = get_source_destination_urls(source_layer_list[dest_loc_type_index], destination_layer_list[dest_loc_type_index])
            dest_output_data_from_query_df, query_dest = query_on_adls(dest_loc_type_index, destination_url + dest_table_location_list[dest_loc_type_index])
            if source_location_type_list[dest_loc_type_index] == ADLS:
                source_output_data_from_query_df, query_source = query_on_adls(dest_loc_type_index, source_url + source_table_location_list[dest_loc_type_index])
            elif source_location_type_list[dest_loc_type_index] == ADB:
                source_output_data_from_query_df, query_source = query_on_adb_sql(source_query_list[dest_loc_type_index])
            sourcedestinationdata.append(get_kpi_data_source_destination(source_output_data_from_query_df, dest_output_data_from_query_df, dest_loc_type_index, query_dest, query_source))

        elif  validation_check_list[dest_loc_type_index] == KPIResult_Check:
            _, destination_url = get_source_destination_urls(destination_layer_list[dest_loc_type_index], destination_layer_list[dest_loc_type_index])
            output_data_from_query_df, query = query_on_adls(dest_loc_type_index, destination_url + dest_table_location_list[dest_loc_type_index])
            kpiresultdata.append(get_kpi_data_kpi_result(dest_loc_type_index, output_data_from_query_df, query))

    elif dest_loc_type == ADB:
        if validation_check_list[dest_loc_type_index] == PrePostRowcount_Check:
            output_data_from_query_df, query = query_on_adb_sql(destination_query_list[dest_loc_type_index])
            prepostdata.append(get_kpi_data_pre_post_run(output_data_from_query_df, dest_loc_type_index, prev_result, status, query, output))
            
        elif validation_check_list[dest_loc_type_index] == Source_DestinationRowcountResult:
            dest_output_data_from_query_df, query_dest = query_on_adb_sql(destination_query_list[dest_loc_type_index])
            if source_location_type_list[dest_loc_type_index] == ADLS:
                source_url, _ = get_source_destination_urls(source_layer_list[dest_loc_type_index], destination_layer_list[dest_loc_type_index])
                source_output_data_from_query_df, query_source = query_on_adls(dest_loc_type_index, source_url + source_table_location_list[dest_loc_type_index])
            elif source_location_type_list[dest_loc_type_index] == ADB:
                source_output_data_from_query_df, query_source = query_on_adb_sql(source_query_list[dest_loc_type_index])
            sourcedestinationdata.append(get_kpi_data_source_destination(source_output_data_from_query_df, dest_output_data_from_query_df, dest_loc_type_index, query_dest, query_source))

        elif  validation_check_list[dest_loc_type_index] == KPIResult_Check:
            output_data_from_query_df, query = query_on_adb_sql(destination_query_list[dest_loc_type_index])
            kpiresultdata.append(get_kpi_data_kpi_result(dest_loc_type_index, output_data_from_query_df, query))

    elif dest_loc_type == SYNAPSE:
        if validation_check_list[dest_loc_type_index] == PrePostRowcount_Check:
            output_data_from_query_df, query = query_on_synapse(destination_query_list[dest_loc_type_index], gold.lower())
            prepostdata.append(get_kpi_data_pre_post_run(output_data_from_query_df, dest_loc_type_index, prev_result, status, query, output))
            
        elif validation_check_list[dest_loc_type_index] == Source_DestinationRowcountResult:
            if source_location_type_list[dest_loc_type_index] == ADB:
                source_output_data_from_query_df, query_source = query_on_adb_sql(source_query_list[dest_loc_type_index])
                dest_output_data_from_query_df, query_dest = query_on_synapse(destination_query_list[dest_loc_type_index], source_layer_list[dest_loc_type_index].lower())
            
            elif source_location_type_list[dest_loc_type_index] == SYNAPSE:
                source_output_data_from_query_df, query_source = query_on_synapse(source_query_list[dest_loc_type_index], gold.lower())
                dest_output_data_from_query_df, query_dest = query_on_synapse(destination_query_list[dest_loc_type_index], gold.lower())
            
            elif source_location_type_list[dest_loc_type_index] == ADLS:
                source_url, _, account = get_PrePostRunsRowcountCheck_locations(source_layer_list[dest_loc_type_index])
                set_spark_conf(account)
                source_output_data_from_query_df, query_source = query_on_adls(dest_loc_type_index, source_url + source_table_location_list[dest_loc_type_index])
                dest_output_data_from_query_df, query_dest = query_on_synapse(destination_query_list[dest_loc_type_index], source_layer_list[dest_loc_type_index].lower())

            sourcedestinationdata.append(get_kpi_data_source_destination(source_output_data_from_query_df, dest_output_data_from_query_df, dest_loc_type_index, query_dest, query_source))
        
        elif  validation_check_list[dest_loc_type_index] == KPIResult_Check:
            output_data_from_query_df, query = query_on_synapse(destination_query_list[dest_loc_type_index], gold.lower())
            kpiresultdata.append(get_kpi_data_kpi_result(dest_loc_type_index, output_data_from_query_df, query))

output += "</Table>"

# COMMAND ----------

# DBTITLE 1,Storing the data into audit DB and into ADFS.
audit_db_df = spark.createDataFrame(data_auditDB, column_audit_db)
append_data_to_audit_db(match_the_schema_with_audit_db(audit_db_df), schema_historical, tableName_historical)

if len(output) > 15:
    dbutils.fs.rm(result_store_location+'/'+StageName+"/"+"PreCheck.xml")
    dbutils.fs.put(result_store_location+'/'+StageName+"/"+"PreCheck.xml", output)

if len(prepostdata) > 0:
    output_data_from_query_df = spark.createDataFrame(prepostdata, pre_post_column)
    overwrite_data_to_layer(output_data_from_query_df, result_store_location+ StageName+ "/" +"Pre-Post Run")
if len(sourcedestinationdata) > 0:
    output_data_from_query_df = spark.createDataFrame(sourcedestinationdata, source_destination_column)
    overwrite_data_to_layer(output_data_from_query_df, result_store_location+ StageName+ "/" +"Source Destination")
if len(kpiresultdata) > 0:
    output_data_from_query_df = spark.createDataFrame(kpiresultdata, kpi_result_column)
    overwrite_data_to_layer(output_data_from_query_df, result_store_location+ StageName+ "/" +"KPI")

for validation_check in set(kpi_summary_info_list):
    summary_data.append([validation_check])
Validation_Summary_df = spark.createDataFrame(summary_data, summary_columns)
overwrite_data_to_layer(Validation_Summary_df, result_store_location+ StageName+ "/" +"Summary Table")