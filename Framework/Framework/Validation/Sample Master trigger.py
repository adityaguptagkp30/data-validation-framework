# Databricks notebook source
pipeline_name_list = [
    "PL_B_CUST_BASE"\
    ,"PL_B_CUST_NATL_ACCT"\
    ,"PL_B_FOBO_TRX"\
    ,"PL_B_NATL_ACCT_BANDC_ITEM"\
    ,"PL_B_PERIOD_XTND"\
    ,"PL_B_PROD_SPLY_SERV"\
    ,"PL_B_RAW_MTRL_COST"\
    ,"PL_B_TPD_CUST"\
    ,"PL_B_TPD_DELIVERY"\
    ,"PL_B_TPD_INVEN_ITEM"\
    ,"PL_B_TPD_LOC"\
    ,"PL_C_CUST_BASE"\
    ,"PL_C_CUST_NATL_ACCT"\
    ,"PL_PCCVol_factBrandVolume"\
    ,'PL_Demo'\
]

pipeline_name_list = [
    "PL_B_FS_EVA_VPU"\
    ,"PL_B_BOSTFS_FSR_CUST_SNAPSHOT_PD"\
    ,"PL_B_BOSTFS_FSR_SCHEDULE"\
    ,"PL_PBC_OPS_IPRO_NET_NEW"\
    ,"PL_B_DOC_AGREE"\
    ,"PL_B_ORG_REL"\
    ,"PL_B_ORG_EX"\
    ,"PL_B_CUST_CLAS"\
    ,"PL_B_CUST_PRFL"\
    ,"PL_B_CUST_TRGT"\
    ,"PL_B_CUST_CLAS_CDE"\
    ,"PL_B_FDSVC_CUST_RTE"\
    ,"PL_B_FDSVC_RTE"\
    ,"PL_B_LOCATION"\
    ,'PL_B_BOL'\
    ,'PL_B_BOL_HDR'\
]

pipeline_name_list = [
    'PL_B_BOL_STAT'\
    ,'PL_B_BUS_TYPE'\
]

check_list = [
    "Bronze data check"
    ,"Silver data check"
    ,"Gold data check"
    # ,"Synapse data check"
    # ,"Pre and post runs rowcount check"
    # ,"KPI data check"
]

# COMMAND ----------

for p_name in pipeline_name_list:
    for check in check_list:
        try:
            dbutils.notebook.run("Master", 1000000, {
                'Environment': "dev"\
                ,'JobID': "1"\
                ,'TaskID': "1"\
                ,'PipelineName':p_name\
                ,'StageName': check
                }
            )
            print(f"""Master Notebook passed for Pipeline Name: {p_name} and Stage Name: {check}""")
        except:
            print(f"""Master Notebook failed for Pipeline Name: {p_name} and Stage Name: {check}""")