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
# MAGIC |Feb 28, 2023   |Aditya Gupta       |Created Data Quality validation notebook for validation framework                                  |                        |
# MAGIC |Mar 01, 2023   |Aditya Gupta       |Added the SLA Check                                                                                |                        |
# MAGIC |Mar 09, 2023   |Aditya Gupta       |Updated the logic of some functons                                                                 |                        |

# COMMAND ----------

environment = getArgument("Environment", "")

# COMMAND ----------

# MAGIC %run ../../Common/Utilities/Utilities

# COMMAND ----------

# DBTITLE 1,Fetching parameters from master notebook
JobID = getArgument("JobID", "")
TaskID = getArgument("TaskID", "")

LocationTypestring = getArgument('LocationType', '')
Location_type_list = create_list_level_3(LocationTypestring)

is_stage_depend_string = getArgument('StageDepend', '')
is_stage_depend = create_list_level_3(is_stage_depend_string)

destination_storage_account_url = getArgument('DestinationTableLocationURL', "")
source_storage_account_url = getArgument('SourceTableLocationURL', "")

MainTableLocationstring = getArgument("DestinationTableLocation", "")
MainTableLocation = create_list_level_3(MainTableLocationstring)

MainTableNamestring = getArgument("TableName","")
MainTableNameList = create_list_level_3(MainTableNamestring)

SubTableLocationstring = getArgument("SourceTableLocation", "")
SubTableLocation = create_list_level_3(SubTableLocationstring)

Result_store_location = getArgument("OutputStoreLocation", "")
ValidationItems1 = getArgument("ValidationItems", "")
ValidationItemsList = create_list_level_3(ValidationItems1)

UnExpectedListColumns1 = getArgument("UnExpectedListColumns", "")
UnExpectedListColumnsList = create_list_level_3(UnExpectedListColumns1)

UnExpectedValuesList1 = getArgument("UnExpectedValuesList", "")
UnExpectedValuesListOfList = create_list_level_3(UnExpectedValuesList1)

NullCheckColumns1 = getArgument("NullCheckColumns", "")
NullCheckColumnsList = create_list_level_3(NullCheckColumns1)

UniqueCheckColumns1 = getArgument("UniqueCheckColumns", "")
UniqueCheckColumnsList = create_list_level_3(UniqueCheckColumns1)

FactTableName1 = getArgument("ForeignKeyFactTableName", "")
FactTableNameList = create_list_level_3(FactTableName1)

DimTableName1 = getArgument("ForeignKeyDimTableName", "")
DimTableNameList = create_list_level_3(DimTableName1)

FactTableNameColumn1 = getArgument("ForeignKeyCheckColumns", "")
FactTableNameColumnList = create_list_level_3(FactTableNameColumn1)

DimTableNameColumn1 = getArgument("ForeignKeyCheckColumns1", "")
DimTableNameColumnList = create_list_level_3(DimTableNameColumn1)

SLACheckColumn1 = getArgument("SLACheckColumn", "")
SLACheckColumnList = create_list_level_3(SLACheckColumn1)

SLA1 = getArgument("SLA", "")
SLAList = create_list_level_3(SLA1)

PipelineName = getArgument("PipelineName", "")

summary_table_created = getArgument("CreatedTableCheck", "")

StageName = getArgument("StageName", "")

OuputFileName = "Summary Table"

# COMMAND ----------

set_spark_conf(get_storage_account_name())

# COMMAND ----------

# DBTITLE 1,Function to Set the Data to the JSON
#Function to set the data into the JSON template for Null Checks
def push_data_to_json_null_check(main_table_name ,null_check_columns):
    json_params['NullCheckList'].append({
        'Schema': "default",
        'TableName': main_table_name ,
        'columns_list': null_check_columns,
    })


#Function to set the data into the JSON template for Unexpected Checks
def push_data_to_json_unexpected_list_check(main_table_name ,unexpected_list_columns,unexpected_values_list):
    for index, _ in enumerate(unexpected_list_columns):
        json_params['UnExpectedList'].append({
            'Schema': "default",
            'TableName': main_table_name ,
            'column_name': unexpected_list_columns[index],
            'unexpected_list': unexpected_values_list[index],
        })

        
#Function to set the data into the JSON template for Unique Checks
def push_data_to_json_unique_check(main_table_name ,unique_check_columns):
    for index, _ in enumerate(unique_check_columns):
        json_params['UniqueCheckList'].append({
            'Schema': "default",
            'TableName': main_table_name ,
            'columns_list': unique_check_columns[index],
        })

        
#Function to set the data into the JSON template for ForeignKey Checks
def push_data_to_json_foreign_key_check(fact_table_name, dim_table_name, fact_table_name_column, dim_table_name_column):
    for index, _ in enumerate(fact_table_name):
        json_params['ForeignKeyCheck'].append({
            'Schema': "default",
            'fact_table_name': fact_table_name[index],
            'dim_table_name': dim_table_name[index],
            'fact_table_name_column': fact_table_name_column[index],
            'dim_table_name_column': dim_table_name_column[index],
        })

        
#Function to set the data into the JSON template for SLA Checks
def push_data_to_json_sla_check(main_table_name , sla_check_column, sla):
    json_params['SLACheckList'].append({
        'Schema': "default",
        'TableName': main_table_name ,
        'column_name': sla_check_column,
        'SLAThreshold': sla,
    })

# COMMAND ----------

# DBTITLE 1,Function to Return the Output from Great Expectation.

def create_df(tablename):
    source_df = spark.sql(f"""select * from {tablename}""")
    spark_df = SparkDFDataset(source_df)
    return spark_df

def checknulls(tablename,list_of_columns):
    source_df = create_df(tablename)
    result_list = []
    for column in list_of_columns:
        result_list.append(source_df.expect_column_values_to_not_be_null(column))
    return result_list


def checkduplicates(tablename,list_of_columns):
    source_df = create_df(tablename)
    if len(list_of_columns)>1:
        result_list = source_df.expect_compound_columns_to_be_unique(list_of_columns)
    else:
        result_list = []
        for column in list_of_columns:
            result_list=source_df.expect_column_values_to_be_unique(column)
    return result_list

def checkunexpectedvalues(tablename,column_name,unexpected_list):
    source_df = create_df(tablename)
    result_list = source_df.expect_column_values_to_not_be_in_set(column_name,unexpected_list) 
    return result_list

def check_foreign_key(dim_table_name, dim_column_name, fact_table_name, fact_column_name):
    result_list=[]
    source_df = create_df(fact_table_name)
    
    df=(spark.sql(f"""select {dim_column_name} from {dim_table_name}"""))
    expected_list=[]
    for i in df.select(df.name).collect():
        expected_list.append(i.name)
    
    result_list = source_df.expect_column_values_to_be_in_set(fact_column_name,expected_list) 
    return result_list

def check_sla( tablename, column_name, sla_threshold):
    source_df = spark.read.format("delta").load(MainTableLocation+tablename)
    source_df.createOrReplaceTempView("Table")
    max_date = get_first_value_from_dataframe_column(spark.sql(f"""SELECT MAX({column_name}) AS Date FROM Table"""), column_name)
    a = my_date_time - max_date.Date
    Hours = (abs(a.days)*24)+((a.seconds+(a.microseconds/1000000))/3600)
    return Hours

# COMMAND ----------

# DBTITLE 1,Segregating the Input Parameters and Adding Data to JSON.
null_table_names = []
unexpected_table_names = []
duplicate_table_names = []
sla_table_names = []
foriegn_table_names = []

unique_validation_checks = []
for index, main_table_name  in enumerate(MainTableNameList):
    if Location_type_list[index] == ADLS:
        MainTable_dataframe = read_delta_files(destination_storage_account_url + MainTableLocation[index])
    elif Location_type_list[index] == ADB:
        MainTable_dataframe = get_data_from_tables(MainTableLocation[index])
    elif Location_type_list[index] == SYNAPSE:
        MainTable_dataframe = read_from_synapse(MainTableLocation[index])
        
    MainTable_dataframe.createOrReplaceTempView('default.'+main_table_name )
    ValidationItems = create_list_level_1(ValidationItemsList[index])
    unexpected_list_columns = create_list_level_1(UnExpectedListColumnsList[index])
    unexpected_values_list = create_list_level_2(UnExpectedValuesListOfList[index])
    null_check_columns = create_list_level_1(NullCheckColumnsList[index])
    unique_check_columns = create_list_level_2(UniqueCheckColumnsList[index])
    fact_table_name = create_list_level_1(FactTableNameList[index])
    dim_table_name = create_list_level_1(DimTableNameList[index])
    fact_table_name_column = create_list_level_1(FactTableNameColumnList[index])
    dim_table_name_column = create_list_level_1(DimTableNameColumnList[index])
    sla_check_column = SLACheckColumnList[index]
    sla = SLAList[index]
    
    for i, _ in enumerate(ValidationItems):
        if ValidationItems[i] == Null_Check:
            null_table_names.append(main_table_name )
            push_data_to_json_null_check(main_table_name ,null_check_columns)
        elif ValidationItems[i] == UnExpected_List:
            unexpected_table_names.append(main_table_name )
            push_data_to_json_unexpected_list_check(main_table_name ,unexpected_list_columns,unexpected_values_list)
        elif ValidationItems[i] == Unique_Check:
            duplicate_table_names.append(main_table_name )
            push_data_to_json_unique_check(main_table_name ,unique_check_columns)
        elif ValidationItems[i] == ForeignKey_Check:
            foriegn_table_names.append(main_table_name )
            push_data_to_json_foreign_key_check(fact_table_name,dim_table_name,fact_table_name_column,dim_table_name_column)
        elif ValidationItems[i] == SLA_Check:
            sla_table_names.append(main_table_name )
            push_data_to_json_sla_check(main_table_name , sla_check_column, sla)
            
        if ValidationItems[i] not in unique_validation_checks:
            unique_validation_checks.append(ValidationItems[i])

# COMMAND ----------

# DBTITLE 1,Create Null Check Output Table and Add data to Summary Table
def create_null_check_output():
    total_null_tables_count = len(null_check_output_list)
    passed_null_tables_count = 0
    fail_null_tables_count = 0
    for i in range(total_null_tables_count):
        check = True
        for j in range(len(null_check_output_list[i])):
            my_date_time = datetime.datetime.now()
            Status = 'Pass' if null_check_output_list[i][j]['success'] else 'Fail'
            Check_for_Null_Table_Data.append([null_table_names[i]\
                                                , my_date_time.strftime("%m-%d-%Y %H:%M")\
                                                , null_check_output_list[i][j]['expectation_config']['kwargs']['column']\
                                                , Status])
            data_auditDB.append(get_audit_db_data_null(JobID\
                                                        , TaskID\
                                                        , PipelineName\
                                                        , StageName\
                                                        , Null_Check\
                                                        , MainTableNameList[i]\
                                                        , null_check_output_list[i][j]['expectation_config']['kwargs']['column']\
                                                        , Status\
                                                        , current_user\
                                                        , my_date_time\
                                                        ))
            if null_check_output_list[i][j]["success"] == 0:
                check = False
        if check:
            passed_null_tables_count = passed_null_tables_count+1
        else:
            fail_null_tables_count = fail_null_tables_count+1
    
    Data.append(["Null Column Validation", total_null_tables_count, passed_null_tables_count, fail_null_tables_count])

# COMMAND ----------

# DBTITLE 1,Create Unexpected List Output Table and Add data to Summary Table
def create_unexpected_list_output():
    total_unexpected_tables_count = len(unexpected_check_output_list)
    passed_unexpected_tables_count = 0
    failed_unexpected_tables_count = 0
    for i in range(total_unexpected_tables_count):
        check = True
        for j in range(len(json_params['UnExpectedList'])):
            if unexpected_table_names[i] == json_params['UnExpectedList'][j]["TableName"] and unexpected_check_output_list[j][1]["success"] == 0:
                check = False
                break
        if check:
            passed_unexpected_tables_count = passed_unexpected_tables_count+1
        else:
            failed_unexpected_tables_count = failed_unexpected_tables_count+1
    
    Data.append(["Unexpected list Validation", total_unexpected_tables_count, passed_unexpected_tables_count, failed_unexpected_tables_count])
    my_date_time = datetime.datetime.now()
    for index, _ in enumerate(unexpected_check_output_list):
        Status = "Pass" if unexpected_check_output_list[index][1]['success'] else 'Fail'
        Check_for_Unexpected_Value_Table_Data.append([unexpected_check_output_list[index][0]\
                                                        , my_date_time.strftime("%m-%d-%Y %H:%M")\
                                                        , unexpected_check_output_list[index][1]["expectation_config"]["kwargs"]['column']\
                                                        , str(unexpected_check_output_list[index][1]["expectation_config"]["kwargs"]['value_set'])[1:-1]\
                                                        , Status])
        data_auditDB.append(get_audit_db_data_unexpected(JobID\
                                                        , TaskID\
                                                        , PipelineName\
                                                        , StageName\
                                                        , UnExpected_List\
                                                        , unexpected_check_output_list[index][0]\
                                                        , unexpected_check_output_list[index][1]["expectation_config"]["kwargs"]['column']\
                                                        , str(unexpected_check_output_list[index][1]["expectation_config"]["kwargs"]['value_set'])[1:-1]\
                                                        , Status\
                                                        , current_user\
                                                        , my_date_time\
                                                        ))

# COMMAND ----------

# DBTITLE 1,Create Unique Check Output Table and Add data to Summary Table
def create_unique_check_output():
    total_duplicate_tables_count = len(duplicate_check_output_list)
    passed_duplicate_tables_count = 0
    fail_duplicate_tables_count = 0
    for i in range(total_duplicate_tables_count):
        check=True
        for j in range(len(json_params["UniqueCheckList"])):
            if duplicate_table_names[i] == json_params["UniqueCheckList"][j]["TableName"] and duplicate_check_output_list[j][1]["success"] == 0:
                check = False
                break
        if check:
            passed_duplicate_tables_count=passed_duplicate_tables_count+1
        else:
            fail_duplicate_tables_count=fail_duplicate_tables_count+1
            
    Data.append(["Duplicates Validation", total_duplicate_tables_count, passed_duplicate_tables_count, fail_duplicate_tables_count])
    my_date_time = datetime.datetime.now()
    for index, _ in enumerate(duplicate_check_output_list):
        Status = "Pass" if duplicate_check_output_list[index][1]['success'] else 'Fail'
        try:
            string = duplicate_check_output_list[index][1]["expectation_config"]["kwargs"]["column"]
            Check_for_Unique_Table_Data.append([duplicate_check_output_list[index][0]\
                                                , my_date_time.strftime("%m-%d-%Y %H:%M")\
                                                , string\
                                                , Status])
        except:
            string = str(duplicate_check_output_list[index][1]["expectation_config"]["kwargs"]["column_list"])
            string = string[1:-1]
            Check_for_Unique_Table_Data.append([duplicate_check_output_list[index][0]\
                                                , my_date_time.strftime("%m-%d-%Y %H:%M")\
                                                , string\
                                                , Status])
        data_auditDB.append(get_audit_db_data_unique(JobID\
                                                    , TaskID\
                                                    , PipelineName\
                                                    , StageName\
                                                    , Unique_Check\
                                                    , duplicate_check_output_list[index][0]\
                                                    , string\
                                                    , Status\
                                                    , current_user\
                                                    , my_date_time\
                                                    ))

# COMMAND ----------

# DBTITLE 1,Create SLA Check Output Table and Add data to Summary Table
def create_sla_check_list_output():
    total_sla_tables_count = len(sla_check_output_list)
    passed_sla_tables_count = 0
    fail_sla_tables_count = 0
    for i in range(total_sla_tables_count):
        if sla_check_output_list[i] < int(sla):
            passed_sla_tables_count = passed_sla_tables_count+1
        else:
            fail_sla_tables_count = fail_sla_tables_count+1
    Data.append(["SLA Check Validation", total_sla_tables_count, passed_sla_tables_count, fail_sla_tables_count])
    my_date_time = datetime.datetime.now()
    for i in range(total_sla_tables_count):
        x=(json_params['SLACheckList'][i]['TableName'])
        y=(json_params['SLACheckList'][i]['column_name'])
        z=('Pass' if sla_check_output_list[i] < 24 else 'Fail')
        Check_for_SLA_Table_Data.append([x, my_date_time.strftime("%m-%d-%Y %H:%M"), y, z])
#         data_auditDB.append()

# COMMAND ----------

# DBTITLE 1,Storing the result into the initialized list from great expectation.
null_check_output_list=[]
unexpected_check_output_list=[]
duplicate_check_output_list=[]
foreignkey_check_output_list=[]
sla_check_output_list=[]
for items in unique_validation_checks:
    if items == Null_Check:
        for i in json_params["NullCheckList"]:
            null_check_output_list.append(checknulls(i['TableName'],i['columns_list']))

    elif items == UnExpected_List:
        for i in json_params["UnExpectedList"]:
            unexpected_check_output_list.append([i['TableName'], checkunexpectedvalues(i['TableName'],i['column_name'],i['unexpected_list'])])

    elif items == Unique_Check: 
        for i in json_params["UniqueCheckList"]:
            duplicate_check_output_list.append([i['TableName'], checkduplicates(i['TableName'],i['columns_list'])])

    elif items == ForeignKey_Check:
        for i in json_params["ForeignKeyCheck"]:
            foreignkey_check_output_list.append(check_foreign_key(i['TableName'],i['columns_list'],i['TableName'],i['columns_list']))

    elif items == SLA_Check:
        for i in json_params["SLACheckList"]:
            sla_check_output_list.append(check_sla(i['TableName'], i['column_name'], i['SLAThreshold'])) 

# COMMAND ----------

# DBTITLE 1,Variable declaration for storing data into DBFS
Columns = ["Validation_Category", "Total_Tables", "Passed", "Failed"]
Data = []
data_auditDB = []

Check_for_Null_Table_Columns=["Tables_Name", "Validated_On", "Column_Name", "Validation_Result"]
Check_for_Null_Table_Data=[]

Check_for_Unique_Table_Columns=["Tables_Name", "Validated_On", "Column_Name", "Validation_Result"]
Check_for_Unique_Table_Data=[]

Check_for_Unexpected_Value_Table_Columns=["Tables_Name", "Validated_On", "Column_Name", "Unexpected_Value", "Validation_Result"]
Check_for_Unexpected_Value_Table_Data=[]

Check_for_SLA_Table_Columns=["Tables_Name", "Validated_On", "Column_Name", "Validation_Result"]
Check_for_SLA_Table_Data=[]

# COMMAND ----------

# DBTITLE 1,Storing data to DBFS.
for items in unique_validation_checks:
    if items == Null_Check: 
        create_null_check_output()
        Dataframe = spark.createDataFrame(Check_for_Null_Table_Data,Check_for_Null_Table_Columns)
        overwrite_data_to_layer(Dataframe, Result_store_location + StageName + "/" + "Null Column Validation")

    elif items == Unique_Check:
        create_unique_check_output()
        Dataframe = spark.createDataFrame(Check_for_Unique_Table_Data,Check_for_Unique_Table_Columns)
        overwrite_data_to_layer(Dataframe, Result_store_location + StageName + "/" + "Duplicates Validation")

    elif items == UnExpected_List:
        create_unexpected_list_output()
        Dataframe = spark.createDataFrame(Check_for_Unexpected_Value_Table_Data,Check_for_Unexpected_Value_Table_Columns)
        overwrite_data_to_layer(Dataframe, Result_store_location + StageName + "/" + "Unexpected list Validation")
        
    elif items == SLA_Check:
        create_sla_check_list_output()
        Dataframe = spark.createDataFrame(Check_for_SLA_Table_Data, Check_for_SLA_Table_Columns)
        overwrite_data_to_layer(Dataframe, Result_store_location + StageName + "/" + "SLA Check Validation")

# COMMAND ----------

# DBTITLE 1,Storing data into audit DB and mapping the IsStageDependency column to audit DB
Validation_summary_df_append = spark.createDataFrame(Data, Columns)
df = spark.createDataFrame(data_auditDB, column_audit_db)
audit_frame = match_the_schema_with_audit_db(df)
for index, main_table_name  in enumerate(MainTableNameList):
    ValidationItems = create_list_level_1(ValidationItemsList[index])
    dependency = create_list_level_1(is_stage_depend[index])
    for ind, check in enumerate(ValidationItems):
        audit_frame = audit_frame.withColumn("ON_STG_DPND",
                                     when(
                                         (col("TBL_NM")==main_table_name) 
                                         &(col("VLDTN_CHCK_TYP")==check)
                                         ,int(dependency[ind])
                                     ).otherwise(col("ON_STG_DPND"))
                                    )
append_data_to_audit_db(audit_frame, schema_historical, tableName_historical)
if summary_table_created:
    Validation_summary_df_append.coalesce(1).write.format("delta").option("delta.columnMapping.mode", "name").mode("append").save(Result_store_location + StageName + "/" + OuputFileName)
else:
    overwrite_data_to_layer(Result_store_location + StageName + "/" +OuputFileName)