# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###### Purpose: Master Notebook for Validation
# MAGIC
# MAGIC ###### Parameter Info:
# MAGIC
# MAGIC ###### Revision History:
# MAGIC
# MAGIC |Date           |Author             |Description                                                                                        |Execution Time          |
# MAGIC |---------------|:-----------------:|---------------------------------------------------------------------------------------------------|------------------------|
# MAGIC |Feb 23, 2023   |Aditya Gupta       |Created master validation notebook for validation framework                                        |                        |
# MAGIC |Feb 24, 2023   |Aditya Gupta       |Updated the trigger notebook functionalities as per the changes made to csv files                  |                        |
# MAGIC |Feb 28, 2023   |Aditya Gupta       |Integrated the Row Count Validation Notebook to the Master Notebook                                |                        |
# MAGIC |Feb 28, 2023   |Aditya Gupta       |Integrated the Data Quality Validation Notebook to the Master Notebook                             |                        |
# MAGIC |Mar 01, 2023   |Aditya Gupta       |Added the Validation Mail Notebook to the Utilites and Added the trigger Mail in Master Notebook   |                        |

# COMMAND ----------

dbutils.widgets.text('Environment','')
environment = dbutils.widgets.get('Environment')

# COMMAND ----------

# DBTITLE 1,Initialize utilities 
# MAGIC %run ../../Common/Utilities/Utilities

# COMMAND ----------

# MAGIC %run ../../Common/Utilities/ValidationMail

# COMMAND ----------

# DBTITLE 1,Create widget to accept params
dbutils.widgets.text('PipelineName','')
PipelineName = dbutils.widgets.get('PipelineName')

dbutils.widgets.text('StageName','')
StageName = dbutils.widgets.get('StageName')

dbutils.widgets.text('JobID','')
JobID = dbutils.widgets.get('JobID')

dbutils.widgets.text('TaskID','')
TaskID = dbutils.widgets.get('TaskID')

# COMMAND ----------

# DBTITLE 1,Define global variables for getting the url's
destination_storage_account_url = set_destination_locations()
source_storage_account_url = set_source_locations()
rowcount_storage_account_url = set_destination_locations()

# COMMAND ----------

# DBTITLE 1,Function to trigger Row Count notebook
# This function is used to set the base paremeters in the string type and adding the commas to these strings for differentiation and the row count validation notebook is triggered.
def trigger_row_count_notebook(row_count_df, rowcount_storage_account_url, source_storage_account_url,destination_storage_account_url):
    Validation_Check_list_RC = create_unique_list(row_count_df,'ValidationCheck')
    Location_Type_string = ""
    Validation_Check_string = ""
    dir_for_row_count_string = ""
    dir_for_source_row_count_string = ""
    dir_for_destination_row_count_string = ""
    row_count_tables_string = ""
    source_row_count_tables_string = ""
    destination_row_count_tables_string = ""
    stage_depend_string = ""

    for V_check in Validation_Check_list_RC:
        Particular_check_df = row_count_df.filter(row_count_df.ValidationCheck == V_check)
        Location_Type_List = get_list_from_dataframe_column(Particular_check_df, "DestinationLocationType")
        stage_depend_list = get_list_from_dataframe_column(Particular_check_df, "IsStageDependency")
        if V_check == Row_Count:
            for locationtype, dependency in zip(Location_Type_List, stage_depend_list):
                Location_Type_string += str(locationtype)+ ","
                stage_depend_string += str(dependency)+","
            
            row_count_tables_string = ",".join(map(str, get_list_from_dataframe_column(Particular_check_df, "RowCountTableName")))
            dir_for_row_count_string = ",".join(map(str, get_list_from_dataframe_column(Particular_check_df, "RowCountTableLocation")))
            Location_Type_string += ","
            stage_depend_string += ","
        else:
            
            for locationtype, dependency in zip(Location_Type_List, stage_depend_list):
                Location_Type_string += str(locationtype)+ ","
                stage_depend_string += str(dependency)+","
            
            source_row_count_tables_string = ",".join(map(str, get_list_from_dataframe_column(Particular_check_df, "SourceTableName")))
            destination_row_count_tables_string = ",".join(map(str, get_list_from_dataframe_column(Particular_check_df, "DestinationTableName")))
            dir_for_source_row_count_string = ",".join(map(str, get_list_from_dataframe_column(Particular_check_df, "SourceTableLocation")))
            dir_for_destination_row_count_string = ",".join(map(str, get_list_from_dataframe_column(Particular_check_df, "DestinationTableLocation")))

            Location_Type_string += ","
            stage_depend_string += ","
    Validation_Check_string = ",".join(map(str, Validation_Check_list_RC))
    Location_Type_string = Location_Type_string[:-2]
    stage_depend_string = stage_depend_string[:-2]
    dbutils.notebook.run(
        "RowCountCheck", 1000000, {
            'Environment': environment\
            ,'JobID': JobID\
            ,'TaskID': TaskID\
            ,'StageDepend': stage_depend_string\
            ,'LocationType': Location_Type_string\
            ,'DestinationTableLocationURL': destination_storage_account_url\
            ,'SourceTableLocationURL': source_storage_account_url\
            ,'RowCountTableLocationURL': rowcount_storage_account_url\
            ,'OutputStoreLocation': output_result_location+PipelineName+'/'\
            ,'ValidationType': Validation_Check_string\
            ,'RowCountTableName':row_count_tables_string\
            ,'SourceRowCountTableName':source_row_count_tables_string\
            ,'DestinationRowCountTableName':destination_row_count_tables_string\
            ,'TableLocationRowCount':dir_for_row_count_string \
            ,'TableLocationSourceRowCount':dir_for_source_row_count_string \
            ,'TableLocationDestinationRowCount':dir_for_destination_row_count_string \
            ,'StageName' : StageName\
            ,'PipelineName' : PipelineName
        }
    )

# COMMAND ----------

# DBTITLE 1,Function to trigger data quality notebook
# This function is used to set the base paremeters in the string type and adding the commas to these strings for differentiation and the Data quality check validation notebook is triggered
def trigger_data_quality_check_notebook(data_quality_check_df,source_storage_account_url,destination_storage_account_url,tables_list, check):
    is_stage_depend_string = ''
    destinationTableLocationstring = ''
    sourceTableLocationstring = ''
    tablestring = ''
    ValidationItemsstring = ''
    UnExpectedListColumnsstring = ''
    UnExpectedValuesListstring = ''
    NullCheckColumnsstring = ''
    UniqueCheckColumnsstring = ''
    ForeignKeyFactTableNamestring = ''
    ForeignKeyDimTableNamestring = ''
    ForeignKeyCheckColumnsstring = ''
    ForeignKeyCheckColumnsstring = ''
    SLACheckColumnstring = ''
    SLAstring = ''
    LocationTypestring = ''
    for table in tables_list:
        tablestring += table+",,,"
        UniqueCheckColumns = None
        UnExpectedListColumns = None
        UnExpectedValuesList = None
        NullCheckColumns = None
        ForeignKeyFactTableName = None
        ForeignKeyDimTableName = None
        ForeignKeyCheckColumns = None
        SLACheckColumn = None
        SLA = None
        filtered_table_df = data_quality_check_df.filter(data_quality_check_df.DestinationTableName == table)
        destinationTableLocation = create_unique_list(filtered_table_df, 'DestinationTableLocation')
        sourceTableLocation = create_unique_list(filtered_table_df, 'SourceTableLocation')
        Location_Type_List = create_unique_list(filtered_table_df, 'DestinationLocationType')
        for dir_source, dir_dest, locationtype in zip(sourceTableLocation, destinationTableLocation, Location_Type_List):
            destinationTableLocationstring += str(dir_dest)+","
            sourceTableLocationstring += str(dir_source)+","
            LocationTypestring += str(locationtype)+","
        
        ValidationChecksList = filtered_table_df.select('ValidationCheck').rdd.flatMap(lambda x: x).collect()
        ValidationItems = ''
        is_stage_depend = ''
        for checks in ValidationChecksList:
            ValidationItems += checks+","
            if checks == UnExpected_List:
                UnExpectedListColumns = get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == UnExpected_List),'columns_list')
                UnExpectedValuesList = get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == UnExpected_List),'unexpected_list')
                is_stage_depend += str(get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == UnExpected_List),'IsStageDependency'))+','
            elif checks == Null_Check:
                NullCheckColumns = get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == Null_Check),'columns_list')
                is_stage_depend += str(get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == Null_Check),'IsStageDependency'))+','
            elif checks == Unique_Check:
                UniqueCheckColumns = get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == Unique_Check),'columns_list')
                is_stage_depend += str(get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == Unique_Check),'IsStageDependency'))+','
            elif checks == ForeignKey_Check:
                ForeignKeyFactTableName = get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == ForeignKey_Check),'SourceTableName')
                ForeignKeyDimTableName = get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == ForeignKey_Check),'DestinationTableName')
                ForeignKeyCheckColumns = get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == ForeignKey_Check),'columns_list')
                is_stage_depend += str(get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == ForeignKey_Check),'IsStageDependency'))+','
            elif checks == 'SLACheck':
                SLACheckColumn = get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == 'SLACheck'),'columns_list')
                SLA = get_first_value_from_dataframe_column(filtered_table_df.filter(filtered_table_df.ValidationCheck == 'SLACheck'),'SLAThreshold')
        
        ValidationItems = ValidationItems[:-1]
        is_stage_depend_string += is_stage_depend + ",,"
        ValidationItemsstring += str(ValidationItems) + ",,,"
        UnExpectedListColumnsstring += str(UnExpectedListColumns) +",,,"
        UnExpectedValuesListstring += str(UnExpectedValuesList) +",,,"
        NullCheckColumnsstring += str(NullCheckColumns) +",,,"
        UniqueCheckColumnsstring += str(UniqueCheckColumns) +",,,"
        ForeignKeyFactTableNamestring += str(ForeignKeyFactTableName) +",,,"
        ForeignKeyDimTableNamestring += str(ForeignKeyDimTableName) +",,,"
        ForeignKeyCheckColumnsstring += str(ForeignKeyCheckColumns) +",,,"
        SLACheckColumnstring += str(SLACheckColumn) +",,,"
        SLAstring += str(SLA) +",,,"
        destinationTableLocationstring += ",,"
        sourceTableLocationstring += ",,"
        LocationTypestring += ",,"
    is_stage_depend_string = is_stage_depend_string[:-3]
    tablestring = tablestring[:-3]
    ValidationItemsstring = ValidationItemsstring[:-3]
    UnExpectedListColumnsstring = UnExpectedListColumnsstring[:-3]
    UnExpectedValuesListstring = UnExpectedValuesListstring[:-3]
    NullCheckColumnsstring = NullCheckColumnsstring[:-3]
    UniqueCheckColumnsstring = UniqueCheckColumnsstring[:-3]
    ForeignKeyFactTableNamestring = ForeignKeyFactTableNamestring[:-3]
    ForeignKeyDimTableNamestring = ForeignKeyDimTableNamestring[:-3]
    ForeignKeyCheckColumnsstring = ForeignKeyCheckColumnsstring[:-3]
    SLACheckColumnstring = SLACheckColumnstring[:-3]
    SLAstring = SLAstring[:-3]
    destinationTableLocationstring = destinationTableLocationstring[:-3]
    sourceTableLocationstring = sourceTableLocationstring[:-3]
    LocationTypestring = LocationTypestring[:-3]
    dbutils.notebook.run(
        "DataQualityCheck", 1000000, {
            'Environment': environment\
            ,'JobID': JobID\
            ,'TaskID': TaskID\
            ,'StageDepend': is_stage_depend_string\
            ,'LocationType': LocationTypestring\
            ,'DestinationTableLocationURL': destination_storage_account_url\
            ,'SourceTableLocationURL': source_storage_account_url\
            ,'DestinationTableLocation': destinationTableLocationstring\
            ,'TableName' :tablestring\
            ,'SourceTableLocation': sourceTableLocationstring\
            ,'OutputStoreLocation':output_result_location+ PipelineName+ '/'\
            ,'ValidationItems': ValidationItemsstring\
            ,'UnExpectedListColumns': UnExpectedListColumnsstring\
            ,'UnExpectedValuesList' : UnExpectedValuesListstring\
            ,'NullCheckColumns' : NullCheckColumnsstring\
            ,'UniqueCheckColumns':UniqueCheckColumnsstring\
            ,'ForeignKeyFactTableName': ForeignKeyFactTableNamestring\
            ,'ForeignKeyDimTableName' : ForeignKeyDimTableNamestring\
            ,'ForeignKeyCheckColumns' : ForeignKeyCheckColumnsstring\
            ,'ForeignKeyCheckColumns1' : ForeignKeyCheckColumnsstring\
            ,'SLACheckColumn' : SLACheckColumnstring\
            ,'SLA' : SLAstring\
            ,'CreatedTableCheck':check\
            ,'StageName':StageName\
            ,'PipelineName' : PipelineName
        }
    )

# COMMAND ----------

# DBTITLE 1,Function to trigger PrePostRunsRowcountCheck Notebook.
# This function is used to set the base paremeters in the string type and adding the commas to these strings for differentiation and the PrePostRunsRowcountCheck notebook is triggered.
def trigger_pre_post_runs_notebook(pre_post_runs_df, layers_list):
    layers = ""
    tables = ""
    tables_location = ""
    location_type_string = ""
    stage_depend_string = ''
    for layer in layers_list:
        layers += layer + ","
        Particular_layer_df = pre_post_runs_df.filter(pre_post_runs_df.DestinationLayer == layer)
        Location_type_list = get_list_from_dataframe_column(Particular_layer_df, "DestinationLocationType")
        dest_table_location_list = get_list_from_dataframe_column(Particular_layer_df, "RowCountTableLocation")
        stage_depend_list = get_list_from_dataframe_column(Particular_layer_df, "IsStageDependency")
        Tables_list = get_list_from_dataframe_column(Particular_layer_df, "RowCountTableName")
        for index, table in enumerate(Tables_list):
            tables += table + ","
            tables_location += dest_table_location_list[index]+","
            location_type_string += Location_type_list[index]+","
            stage_depend_string += stage_depend_list[index]+','
        tables += ","
        tables_location += ","
        location_type_string += ","
        stage_depend_string += ','
    tables = tables[:-2]
    tables_location = tables_location[:-2]
    location_type_string = location_type_string[:-2]
    stage_depend_string = stage_depend_string[:-2]
    layers = layers[:-1]
    dbutils.notebook.run(
        "PrePostRunsRowcountCheck", 1000000, {
            'Environment': environment\
            ,'JobID': JobID\
            ,'TaskID': TaskID\
            ,'Layers':layers\
            ,'Tables':tables\
            ,'PipelineName':PipelineName\
            ,'LocationType': location_type_string\
            ,'TableLocation':tables_location\
            ,'StageDepend': stage_depend_string\
            ,'OutputStoreLocation': output_result_location + str(PipelineName)+'/'
        }
    )

# COMMAND ----------

def trigger_kpi_validation_notebook(kpi_check_df):
    unique_id_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "UniqueID")))
    source_table_location_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "SourceTableLocation")))
    dest_table_location_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "DestinationTableLocation")))
    destination_location_type_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "DestinationLocationType")))
    source_location_type_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "SourceLocationType")))
    kpi_name_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "KPIName")))
    destination_layer_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "DestinationLayer")))
    source_layer_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "SourceLayer")))
    source_query_string = ",,,".join(map(str, get_list_from_dataframe_column(kpi_check_df, "SourceQuery")))
    destination_query_string = ",,,".join(map(str, get_list_from_dataframe_column(kpi_check_df, "DestinationQuery")))
    aggregate_function_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "AggregateFunction")))
    validation_check_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "ValidationCheck")))
    table_column_name_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "columns_list")))
    destination_table_name_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "DestinationTableName")))
    source_table_name_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "SourceTableName")))
    stage_depend_string = ",".join(map(str, get_list_from_dataframe_column(kpi_check_df, "IsStageDependency")))
    
    dbutils.notebook.run(
        "KPIValidationCheck", 1000000, {
            'Environment': environment\
            ,'JobID': JobID\
            ,'TaskID': TaskID\
            ,'PipelineName':PipelineName\
            ,'UniqueID': unique_id_string\
            ,'DestinationLocationType': destination_location_type_string\
            ,'SourceLocationType': source_location_type_string\
            ,'KPIName': kpi_name_string\
            ,'DestinationTableLocation': dest_table_location_string\
            ,'SourceTableLocation': source_table_location_string\
            ,'DestinationLayer': destination_layer_string\
            ,'SourceLayer': source_layer_string\
            ,'SourceQuery':source_query_string\
            ,'DestinationQuery':destination_query_string\
            ,'AggregateFunction': aggregate_function_string\
            ,'ValidationCheck': validation_check_string\
            ,'DestinationTableName': destination_table_name_string\
            ,'SourceTableName': source_table_name_string\
            ,'ColumnName': table_column_name_string\
            ,'StageDepend': stage_depend_string\
            ,'OutputStoreLocation': output_result_location + str(PipelineName)+'/'
        }
    )

# COMMAND ----------

# DBTITLE 1,Creating Data Frames for Row Count and Data Quality Validation Notebook.
configuration_df = read_csv_files(get_storage_uri(environment,'configs') + input_csv_path, delimeter)
configuration_df = configuration_df.filter((configuration_df.StageName == StageName) & (configuration_df.PipelineName == PipelineName) & (configuration_df.Check_Status == "yes"))
if configuration_df.count() == 0:
    dbutils.notebook.exit(f"""Error: No input data in the validation excel for {PipelineName} on {StageName}""".format({PipelineName}, {StageName}))
elif StageName == PrePostRunsRowcountCheck_Stage:
    layers_list = create_unique_list(configuration_df, 'DestinationLayer')
    trigger_pre_post_runs_notebook(configuration_df, layers_list)
    summary_df = spark.read.format("delta").load(output_result_location+PipelineName+"/"+StageName+"/Summary Table")
    HTML = generate_html_string_pre_post_runs(summary_df, PipelineName, StageName, output_result_location+PipelineName+"/"+StageName+"/", project_name, "Pre Post Runs")
elif StageName == KPI_stage:
    trigger_kpi_validation_notebook(configuration_df)
    summary_df = spark.read.format("delta").load(output_result_location+PipelineName+"/"+StageName+"/Summary Table")
    HTML = generate_html_string(summary_df, PipelineName, StageName, " ", " ", output_result_location+PipelineName+"/"+StageName+"/", project_name)
else:
    row_count_df = configuration_df.filter((configuration_df.ValidationCheck == Row_Count) | (configuration_df.ValidationCheck == Source_DestinationRowcountResult))
    data_quality_check_df = configuration_df.filter((configuration_df.ValidationCheck != Row_Count) & (configuration_df.ValidationCheck != Source_DestinationRowcountResult))
    Tables_list_DQ = create_unique_list(data_quality_check_df,'DestinationTableName')
    if row_count_df.count() == 0 and data_quality_check_df.count() != 0:
        trigger_data_quality_check_notebook(data_quality_check_df,source_storage_account_url,destination_storage_account_url,Tables_list_DQ, False)
    elif data_quality_check_df.count() == 0 and row_count_df.count() != 0:
        trigger_row_count_notebook(row_count_df, rowcount_storage_account_url, source_storage_account_url,destination_storage_account_url)
    elif row_count_df.count()!=0 and data_quality_check_df.count()!=0:
        trigger_row_count_notebook(row_count_df, rowcount_storage_account_url, source_storage_account_url,destination_storage_account_url)
        trigger_data_quality_check_notebook(data_quality_check_df, source_storage_account_url,destination_storage_account_url,Tables_list_DQ, True)
    else:
        dbutils.notebook.exit(f"""Error: No input data in the validation excel for {PipelineName} on {StageName}""".format({PipelineName}, {StageName}))
    summary_df = spark.read.format("delta").load(output_result_location+PipelineName+"/"+StageName+"/Summary Table")
    Container = get_container_name()
    Storage_Account = get_storage_account_name()
    HTML = generate_html_string(summary_df, PipelineName, StageName, Container, Storage_Account, output_result_location + PipelineName+"/"+StageName+"/", project_name)
sendmail(HTML, PipelineName, StageName, project_name)