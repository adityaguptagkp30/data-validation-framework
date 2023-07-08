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
# MAGIC |Mar 03, 2023   |Rohit Singla       |Returns the validation status between the previous and current run of the pipelines                |                        |

# COMMAND ----------

environment = getArgument("Environment", "")

# COMMAND ----------

# MAGIC %run ../../Common/Utilities/Utilities

# COMMAND ----------

JobID = getArgument("JobID", "")
TaskID = getArgument("TaskID", "")
Layers_string = getArgument("Layers", "")
Layers_list = create_list_level_1(Layers_string)

Tables_string = getArgument("Tables", "")
Tables_list = create_list_level_2(Tables_string)

PipelineName = getArgument("PipelineName", "")

location_type_string = getArgument('LocationType', "")
Location_Type_list = create_list_level_2(location_type_string)

Table_Location_string = getArgument("TableLocation", "")
Table_Location_List = create_list_level_2(Table_Location_string)

stage_depend_string = getArgument("StageDepend", "")
stage_depend_list = create_list_level_2(stage_depend_string)

Result_store_location = getArgument("OutputStoreLocation", "")

StageName = PrePostRunsRowcountCheck_Stage

# COMMAND ----------

# DBTITLE 1,Sets the configuration of the layers present in layer column of input excel
for index, layer in enumerate(Layers_list):
    for locationtype in Location_Type_list[index]:
        if locationtype == ADLS:
            set_spark_conf(get_storage_account_name_pre_post_runs(layer))
            break

# COMMAND ----------

# DBTITLE 1,Compare the current data to the previous data stored in xml file.
def compare_to_stored_xml(xml_location, table_name, destination_count, layer, pass_count, fail_count):
    tree = ET.parse(xml_location)
    root_tag = tree.getroot()
    for row in root_tag:
        if row[0].text == layer:
            if row[1].text == table_name:
                Pass_Count, Fail_Count, Status = threshold(int(row[2].text), destination_count, pass_count, fail_count)
                return int(row[2].text), Pass_Count, Fail_Count, Status
    if destination_count == 0:
        return 0, Pass_Count, Fail_Count+1, False
    return 0, Pass_Count+1, Fail_Count, True

# COMMAND ----------

# DBTITLE 1,It is the Driver code for this check which creates the data into DBFS, audit DB and creating.
output = "<Table>"
data_auditDB = []
Row_count_table = []
Data = []
Summary_table_Columns = ["Layer", "Container", "Storage_Account", "Total_Tables", "Passed", "Failed"]
Columns = ["Layer", "Table_Name", "Validated_On", "Previous_Row_Count", "Current_Row_count", "Validation_Result"]
for index, layer in enumerate(Layers_list):
    pass_count = 0
    fail_count = 0
    Table_count = 0
    Location_link, Container, Account = get_PrePostRunsRowcountCheck_locations(layer)
    for i, table_name in enumerate(Tables_list[index]):
        Location_URL = Location_link + Table_Location_List[index][i]
        Table_count = Table_count+1
        pre_post_list = []
        Status = ""
        pre_post_list.append(layer) 
        pre_post_list.append(table_name)
        my_date_time = datetime.datetime.now()
        pre_post_list.append(my_date_time.strftime("%m-%d-%Y %H:%M"))
        if Location_Type_list[index][i] == ADLS:
            Table_DF = read_delta_files(Location_URL)
        elif Location_Type_list[index][i] == ADB:
            Table_DF = get_data_from_tables(Table_Location_List[index][i])
        elif Location_Type_list[index][i] == SYNAPSE:
            Table_DF = read_from_synapse(Table_Location_List[index][i])
            
        Table_DF_Row_Count = Table_DF.count()
        output += f"""<Row><Layer>{layer}</Layer><TableName>{table_name}</TableName><Count>{Table_DF_Row_Count}</Count></Row>""".format({layer}, {table_name}, {Table_DF_Row_Count})
        if os.path.exists("/dbfs"+Result_store_location+StageName+"/"+"PreCheck.xml"):
            Previous_Count, pass_count, fail_count, Status = compare_to_stored_xml("/dbfs"+Result_store_location+StageName+"/"+"PreCheck.xml", table_name, Table_DF_Row_Count, layer, pass_count, fail_count)
            pre_post_list.append("{:,}".format(Previous_Count))
        else:
            pre_post_list.append("0")
            if Table_DF_Row_Count == 0:
                Status = "Fail"
                fail_count = fail_count+1
            else:
                Status = "Pass"
                pass_count = pass_count+1
        pre_post_list.append("{:,}".format(Table_DF_Row_Count))
        pre_post_list.append(Status)
        
        data_auditDB.append(
            get_pre_post_runs_audit_db_data(
                JobID\
                , TaskID\
                , PipelineName\
                , PrePostRunsRowcountCheck_Stage\
                , PrePostRowcount_Check\
                , layer\
                , table_name\
                , "null"\
                , str(pre_post_list[-3].replace(",", ""))\
                , str(Table_DF_Row_Count)\
                , Status\
                , current_user\
                , my_date_time\
                ,int(stage_depend_list[index][i])
            )
        )
        Row_count_table.append(pre_post_list)
    
    Data.append([layer, Container, Account, Table_count, pass_count, fail_count])
Dataframe = spark.createDataFrame(Row_count_table, Columns)
df = spark.createDataFrame(data_auditDB, column_audit_db)
append_data_to_audit_db(match_the_schema_with_audit_db(df), schema_historical, tableName_historical)
output +="</Table>"

dbutils.fs.rm(Result_store_location+PrePostRunsRowcountCheck_Stage+"/"+"PreCheck.xml")
dbutils.fs.put(Result_store_location+PrePostRunsRowcountCheck_Stage+"/"+"PreCheck.xml", output)

# COMMAND ----------

Validation_Summary_df = spark.createDataFrame(Data, Summary_table_Columns)

# COMMAND ----------

# DBTITLE 1,Storing data to DBFS
overwrite_data_to_layer(Validation_Summary_df, Result_store_location+ PrePostRunsRowcountCheck_Stage+ "/" +"Summary Table")
overwrite_data_to_layer(Dataframe, Result_store_location + PrePostRunsRowcountCheck_Stage+"/" + "Pre Post Runs")