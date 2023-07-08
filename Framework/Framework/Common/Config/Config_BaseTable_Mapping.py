# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ###### Purpose: Configuration Variables for every notebook using incremental pull
# MAGIC
# MAGIC ###### Parameter Info:
# MAGIC
# MAGIC ###### Revision History:
# MAGIC
# MAGIC |Date           |Author              |Description                                                                                        |Execution Time          |
# MAGIC |---------------|:------------------:|---------------------------------------------------------------------------------------------------|------------------------|
# MAGIC |Apr 03, 2023   |Aditya Gupta        |Added table detail for primary keys in json format                                                 |                        |
# MAGIC

# COMMAND ----------

# DBTITLE 1,Table Name with PK, ADLS Paths
#pylint: disable=unused-variable
Table_detail = {
    "B_CUST_BASE": {
        "primary_keys": [
            "CUST_ID"
        ],
        "bronze": "/bronzepath/schemaname/tablename",
        "silver": "silverpath/schemaname/tablename",
        "gold": "/goldpath/schemaname/tablename"
    },
    
    "B_CUST_CLAS": {
        "primary_keys": [
            "CUST_ID" ,
            "CUST_CLAS_CDE" ,
            "CUST_CLAS_EFF_DTE"
        ],
        "bronze": "/bronzepath/schemaname/tablename"
    },
   
}