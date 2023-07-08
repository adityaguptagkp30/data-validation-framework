# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ###### Purpose: For Sending Mail Notification
# MAGIC
# MAGIC ###### Parameter Info:
# MAGIC
# MAGIC ###### Revision History:
# MAGIC
# MAGIC |Date           |Author             |Description                                                                                        |
# MAGIC |---------------|:-----------------:|---------------------------------------------------------------------------------------------------|
# MAGIC |Feb 28, 2023   |Rohit Singla       |Sending Mail Notifcation for validation framework                                                  |
# MAGIC |Mar 03, 2023   |Rohit Singla       |Added sending Mail Notifcation for validation framework for PrePostRunsRowCount                    |

# COMMAND ----------

# MAGIC %run ../Config/Config_Mail_Alerts

# COMMAND ----------

# DBTITLE 1,Function to Generate the CSS part.
def create_css():
    Message = ""
    Message += "<style>\n\t\t\t"
    Message += "* {\n\t\t\t\tfont-family: sans-serif;\n\t\t\t\tmargin: 0px;\n\t\t\t\tpadding: 0;\n\t\t\t}\n\t\t\t"
    Message += "table {\n\t\t\t\tborder: 1px solid black;\n\t\t\t\tmargin-bottom: 16px;\n\t\t\t\tborder-collapse: collapse;\n\t\t\t\tmargin-left: 10px;\n\t\t\t}\n\t\t\t"
    Message += "h1 {\n\t\t\t\ttext-align: center;\n\t\t\t\tpadding: 8px 0;\n\t\t\t\tborder-top: 3px solid black;\n\t\t\t\tborder-bottom: 3px solid black;\n\t\t\t\tmargin-bottom: 16px;\n\t\t\t}\n\t\t\t"
    Message += "th {\n\t\t\t\tborder: 1px solid black;\n\t\t\t\tpadding: 10px;\n\t\t\t}\n\t\t\t"
    Message += "td {\n\t\t\t\tborder: 1px solid black;\n\t\t\t\tpadding: 10px;\n\t\t\t\tfont-weight: 500;\n\t\t\t}\n\t\t\t"
    Message += ".projectTitle {\n\t\t\t\tpadding: 10px;\n\t\t\t}\n\t\t\t"
    Message += ".pipelineInformation {\n\t\t\t\tborder-top: 1px solid white;\n\t\t\t\tpadding: 10px;\n\t\t\t}\n\t\t\t"
    Message += ".subHeading {\n\t\t\t\tpadding: 10px;\n\t\t\t}\n\t\t\t"
    Message += ".matrixTableRowHeading {\n\t\t\t\tpadding: 10px;\n\t\t\t\tfont-weight: bold;\n\t\t\t}\n\t\t\t"
    Message += "div {\n\t\t\t\tpadding: 10px;\n\t\t\t}\n\t\t\t"
    Message += "\n\t\t</style>"
    return Message

# COMMAND ----------

# DBTITLE 1,Functions to Generate the HTML code for the Mail.
# Function to generate the HTML code for the the information table in the mail.
def created_validation_run_information_table(pipeline_name, stage_name, container, storage_account):
    Message = ""
    Message += """\n\t\t\t<table>"""
    Message += "\n\t\t\t\t<tbody>"
    Message += "\n\t\t\t\t\t<tr>"
    Message += """\n\t\t\t\t\t\t<td class="matrixTableRowHeading">Pipeline Name:</td>"""
    Message += f"""\n\t\t\t\t\t\t<td>{pipeline_name}</td>""".format(pipeline_name)
    Message += "\n\t\t\t\t\t</tr>"
    Message += "\n\t\t\t\t\t<tr>"
    Message += """\n\t\t\t\t\t\t<td class="matrixTableRowHeading">Stage Name:</td>"""
    Message += f"""\n\t\t\t\t\t\t<td>{stage_name}</td>""".format(stage_name)
    Message += "\n\t\t\t\t\t</tr>"
    Message += "\n\t\t\t\t\t<tr>"
    Message += """\n\t\t\t\t\t\t<td class="matrixTableRowHeading">Container:</td>"""
    Message += f"""\n\t\t\t\t\t\t<td>{container}</td>""".format(container)
    Message += "\n\t\t\t\t\t</tr>"
    Message += "\n\t\t\t\t\t<tr>"
    Message += """\n\t\t\t\t\t\t<td class="matrixTableRowHeading">Storage Account:</td>"""
    Message += f"""\n\t\t\t\t\t\t<td>{storage_account}</td>""".format(storage_account)
    Message += "\n\t\t\t\t\t</tr>"
    Message += "\n\t\t\t\t</tbody>"
    Message += """\n\t\t\t</table>"""
    return Message
  

# Function to generate the HTML code for printing the summary table on mail and returning the list of validation checks in the summary table.
def create_summary_html_table(summary_df):
    count = 0
    List_of_Validation_Category = []
    Message = ""
    Message += """\n\t\t\t<table>"""
    Message+= """\n\t\t\t\t<thead>"""
    Message+= """\n\t\t\t\t\t<th>#</th>"""
    for index in range(len(summary_df.columns)):
        Message+= "\n\t\t\t\t\t<th>"+ summary_df.columns[index].replace("_", " ") +"</th>"
    Message+= """\n\t\t\t\t</thead>"""
    
    Message+= """\n\t\t\t\t<tbody>"""
    for row_index in summary_df.collect():
        List_of_Validation_Category.append(str(row_index[0]))
        Message+= "\n\t\t\t\t\t<tr>"
        count=count+1
        Message+= f"""\n\t\t\t\t\t\t<td>{count}</td>""".format(count)
        for i in range(len(summary_df.columns)):
            Message+= "\n\t\t\t\t\t\t<td>" + str(row_index[i]) + "</td>"
        Message+= "\n\t\t\t\t\t</tr>"
    Message+= """\n\t\t\t\t</tbody>"""
    
    Message += """\n\t\t\t</table>"""
    if StageName == KPI_stage:
        Message = ""
    return Message, List_of_Validation_Category

# This function is used to create the HTML code for printing the tables in the mail based upon the checks in the summary table.
def create_tables_from_list(result_store_location, pipeline_name, stage_name, validation_category):
    count = 0
    Message = ""
    Message+= f"""\n\t\t\t<h2 class="subHeading">{validation_category} results: </h2>""".format(validation_category)
    Dataframe = spark.read.format("delta").load(result_store_location +validation_category)
    Message += """\n\t\t\t<table>"""
    Message+= """\n\t\t\t\t<thead>"""
    Message+= """\n\t\t\t\t\t<th>#</th>"""
    for ColumnNameIndex in range(len(Dataframe.columns)):
        Message+= "\n\t\t\t\t\t<th>"+ Dataframe.columns[ColumnNameIndex].replace("_", " ") +"</th>"
    Message+= """\n\t\t\t\t</thead>"""
    
    Message+= """\n\t\t\t\t<tbody>"""
    for row_index in Dataframe.collect():
        Message+= "\n\t\t\t\t\t<tr>"
        count=count+1
        Message+= f"""\n\t\t\t\t\t\t<td>{count}</td>""".format(count)
        for i in range(len(Dataframe.columns)):
            Message+= "\n\t\t\t\t\t\t<td>" + str(row_index[i]) + "</td>"
        Message+= "\n\t\t\t\t\t</tr>"

    Message+= """\n\t\t\t\t</tbody>"""
    
    Message += """\n\t\t\t</table>"""
    return Message


# This is the main function which generates the whole HTML code for the mail.
def generate_html_string(validation_summary_df, pipeline_name, stage_name, container, storage_account, result_store_location, subject_prefix):
    Mail_Notification = ""
    Mail_Notification = "<html>"
    Mail_Notification += "\n\t<head>\n\t\t"
    msg_css = create_css()
    Mail_Notification += msg_css
    Mail_Notification += "\n\t</head>"
#     Till Here we have CSS part
    Mail_Notification += "\n\t<body>\n\t\t<div>"
    Mail_Notification += f"""\n\t\t\t<h1 class="projectTitle">[{subject_prefix}] [{env}]: [{pipeline_name}] {stage_name} Execution Status as on {current_date} (EST)</h1>""".format({subject_prefix}, {env}, {pipeline_name}, {stage_name}, {current_date})
#     Created Validated Run Information table
    Mail_Notification += """\n\t\t\t<h2 class="subHeading"> Validation Run Information: </h2>"""
    if stage_name == Synapse_stage or stage_name == KPI_stage:
        Mail_Notification += created_validation_run_information_table_prepostruns_and_synapse(pipeline_name, stage_name)
    else:
        Mail_Notification += created_validation_run_information_table(pipeline_name, stage_name, container, storage_account)
    
#     Created Validation Summary table
    if StageName != KPI_stage:
        Mail_Notification += """\n\t\t\t<h2 class="subHeading">Validation Summary: </h2>"""
    Message, List_of_Validation_Category = create_summary_html_table(validation_summary_df)
    Mail_Notification += Message
    
#     Creating All the Tables from the List
    for validation_category in List_of_Validation_Category:
        Mail_Notification+= create_tables_from_list(result_store_location, pipeline_name, stage_name, validation_category)
    
    Mail_Notification+= f"""\n\t\t\t<p>Best Regards, <br>{subject_prefix} Team </p>""".format({subject_prefix})
    Mail_Notification+= "\n\t\t\t<br>"
    Mail_Notification+= "\n\t\t\t<p>*Do Not Reply to this Mail, as It is a System Generated Mail.</b> </p>"
    Mail_Notification+= "\n\t\t</div>"
    Mail_Notification+= "\n\t</body>"
    Mail_Notification+= "\n</html>"
    return Mail_Notification

# COMMAND ----------

# DBTITLE 1,Return the HTML string of the Tables of PrePostRuns.
# Function to generate the HTML code for the the information table in the mail.
def created_validation_run_information_table_prepostruns_and_synapse(pipeline_name, stage_name):
    Message = ""
    Message += """\n\t\t\t<table>"""
    Message += "\n\t\t\t\t<tbody>"
    Message += "\n\t\t\t\t\t<tr>"
    Message += """\n\t\t\t\t\t\t<td class="matrixTableRowHeading">Pipeline Name:</td>"""
    Message += f"""\n\t\t\t\t\t\t<td>{pipeline_name}</td>""".format(pipeline_name)
    Message += "\n\t\t\t\t\t</tr>"
    Message += "\n\t\t\t\t\t<tr>"
    Message += """\n\t\t\t\t\t\t<td class="matrixTableRowHeading">Stage Name:</td>"""
    Message += f"""\n\t\t\t\t\t\t<td>{stage_name}</td>""".format(stage_name)
    Message += "\n\t\t\t\t\t</tr>"
    Message += "\n\t\t\t\t</tbody>"
    Message += """\n\t\t\t</table>"""
    return Message


# This function is used to Generate the HTML code for the summary table of PrePostRuns.
def create_summary_table_pre_post_runs(summary_df):
    count = 0
    Message = ""
    Message += """\n\t\t\t<table>"""
    Message+= """\n\t\t\t\t<thead>"""
    Message+= """\n\t\t\t\t\t<th>#</th>"""
    for index in range(len(summary_df.columns)):
        ColumnName = summary_df.columns[index].replace("_", " ")
        Message+= "\n\t\t\t\t\t<th>"+ ColumnName +"</th>"
    Message+= """\n\t\t\t\t</thead>"""
    
    Message+= """\n\t\t\t\t<tbody>"""
    for row_index in summary_df.collect():
        Message+= "\n\t\t\t\t\t<tr>"
        count=count+1
        Message+= f"""\n\t\t\t\t\t\t<td>{count}</td>""".format(count)
        for i in range(len(summary_df.columns)):
            Message+= "\n\t\t\t\t\t\t<td>" + str(row_index[i]) + "</td>"
        Message+= "\n\t\t\t\t\t</tr>"
    Message+= """\n\t\t\t\t</tbody>"""
    Message += """\n\t\t\t</table>"""
    return Message


# This function is used to return the HTML code for the the entries into the summary table.
def create_tables_for_pre_post_runs(result_store_location):
    count = 0
    Message = ""
    Message+= """\n\t\t\t<h2 class="subHeading">Pre Post Runs Row Count Results: </h2>"""
    Dataframe = spark.read.format("delta").load(result_store_location)
    Message += """\n\t\t\t<table>"""
    Message+= """\n\t\t\t\t<thead>"""
    Message+= """\n\t\t\t\t\t<th>#</th>"""
    for ColumnNameIndex in range(len(Dataframe.columns)):
        Message+= "\n\t\t\t\t\t<th>"+ Dataframe.columns[ColumnNameIndex].replace("_", ' ') +"</th>"
    Message+= """\n\t\t\t\t</thead>"""
    
    Message+= """\n\t\t\t\t<tbody>"""
    for row_index in Dataframe.collect():
        Message+= "\n\t\t\t\t\t<tr>"
        count=count+1
        Message+= f"""\n\t\t\t\t\t\t<td>{count}</td>""".format(count)
        for i in range(len(Dataframe.columns)):
            Message+= "\n\t\t\t\t\t\t<td>" + str(row_index[i]) + "</td>"
        Message+= "\n\t\t\t\t\t</tr>"

    Message+= """\n\t\t\t\t</tbody>"""
    
    Message += """\n\t\t\t</table>"""
    return Message



# This function is used to generate the whole HTML code for the mail.
def generate_html_string_pre_post_runs(validation_summary_df, pipeline_name, stage_name, result_store_location, subject_prefix, data_table_name):
    Mail_Notification = ""
    Mail_Notification = "<html>"
    Mail_Notification += "\n\t<head>\n\t\t"
    Mail_Notification += create_css()
    Mail_Notification += "\n\t</head>"
#     Till Here we have CSS part

    Mail_Notification += "\n\t<body>\n\t\t<div>"
    Mail_Notification += f"""\n\t\t\t<h1 class="projectTitle">[{subject_prefix}] [{env}]: [{pipeline_name}] {stage_name} Execution Status as on {current_date} (EST)</h1>""".format({subject_prefix}, {env}, {pipeline_name}, {stage_name}, {current_date})
    
#     Created Validated Run Information table
    Mail_Notification += """\n\t\t\t<h2 class="subHeading"> Validation Run Information: </h2>"""
    Mail_Notification += created_validation_run_information_table_prepostruns_and_synapse(pipeline_name, stage_name)
#     Created Validation Summary table
    Mail_Notification += """\n\t\t\t<h2 class="subHeading">Validation Summary: </h2>"""
    Mail_Notification += create_summary_table_pre_post_runs(validation_summary_df)

    Mail_Notification+= create_tables_for_pre_post_runs(result_store_location + data_table_name)
    
    Mail_Notification+= f"""\n\t\t\t<p>Best Regards, <br>{subject_prefix} Team </p>""".format({subject_prefix})
    Mail_Notification+= "\n\t\t\t<br>"
    Mail_Notification+= "\n\t\t\t<p>*Do Not Reply to this Mail, as It is a System Generated Mail.</b> </p>"
    Mail_Notification+= "\n\t\t</div>"
    Mail_Notification+= "\n\t</body>"
    Mail_Notification+= "\n</html>"
    return Mail_Notification

# COMMAND ----------

# DBTITLE 1,Function to send the Mail Notification.
def sendmail(message, pipeline_name, stage_name, subject_prefix):
    sendEmailGeneric(secret= email_secret\
                     ,emailfrom=MailSenderEmailId\
                     ,subject=f"""[{subject_prefix}] [{env}]: [{pipeline_name}] {stage_name} Execution Status as on {current_date} (EST)""".format({subject_prefix}, {env}, {pipeline_name}, {stage_name}, {current_date}) \
                     ,message=message\
                     ,receiver = MailRecipientEmailId
                    )