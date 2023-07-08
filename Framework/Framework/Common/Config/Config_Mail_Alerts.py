# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###### Purpose: Configurations for sender and reciever E-mail IDs
# MAGIC
# MAGIC ###### Parameter Info:
# MAGIC
# MAGIC ###### Revision History:
# MAGIC
# MAGIC |Date           |Author             |Description                                                                                        |Execution Time          |
# MAGIC |---------------|:-----------------:|---------------------------------------------------------------------------------------------------|------------------------|
# MAGIC |Mar 22, 2023   |Rohit Singla       |Created the notebook for getting the details of the users for sender and reciever                  |                        |

# COMMAND ----------

current_date = datetime.datetime.now().strftime("%b %d, %Y")

# COMMAND ----------

# DBTITLE 1,Getting the emails of the sender and the receiver from the audit DB.
try:    
    sender_reciever_info_df = read_from_audit_db(schema_notification, tablename_notification)
    sender_reciever_info_df = sender_reciever_info_df.filter((sender_reciever_info_df.ENV == environment) & (sender_reciever_info_df.TYP == 'Notification'))
    sender_reciever_info_json_string = get_first_value_from_dataframe_column(sender_reciever_info_df, 'CNFGRTN_MPPNG_VAL')
    sender_reciever_info_json = json.loads(sender_reciever_info_json_string)
    MailSenderEmailId = sender_reciever_info_json['from']
    MailRecipientEmailId = sender_reciever_info_json['to']
except:
    MailRecipientEmailIdList = []
    MailSenderEmailId = ""
    if environment == 'dev':
        MailSenderEmailId = "admin@gmail.com"
        MailRecipientEmailIdList = [
            "abc@gmail.com"\
            ,"pqr@gmail.com"\
        ]
    elif environment == 'uat':
        MailSenderEmailId = "admin@gmail.com"
        MailRecipientEmailIdList = [
            "abc@gmail.com"\
            ,"pqr@gmail.com"\
        ]
    elif environment == 'prod':
        MailSenderEmailId = "admin@gmail.com"
        MailRecipientEmailIdList = [
             "abc@gmail.com"\
            ,"pqr@gmail.com"\
        ]
    MailRecipientEmailId = ""
    for ids in MailRecipientEmailIdList:
        MailRecipientEmailId = MailRecipientEmailId+ids+";"
    MailRecipientEmailId = MailRecipientEmailId[:-1]

# COMMAND ----------

if environment == 'dev':
    email_secret = dbutils.secrets.get('Dev-Cred','email-secret')
elif environment == 'uat':
    email_secret = dbutils.secrets.get('UAT-Cred','email-secret')
elif environment == 'prod':
    email_secret = dbutils.secrets.get('Prod-Cred','email-secret')

# COMMAND ----------

if environment == 'dev':
    env = "Development"
elif environment == 'uat':
    env = "Pre-Production"
elif environment == 'prod':
    env = "Production"