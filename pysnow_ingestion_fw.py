"""
    pysnow_ingestion_fw.py
    ---------------------------------------
    This module contain Generic Ingestion Logic , which takes data from ORACLE or SQL Server & load it to hive tables

    Author:  CPG8084 - Sheik Mohaideen Sharbudeen
             CPG8019 - Satadru Mukherjee


    LOB: SC

    pre-requisite: Snowflake Insert Query

    For details , refer this wiki : https://github.thehartford.com/HIG/edo_ops_frameworks/wiki

    -----------------------------------------
"""
import requests, json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit, current_timestamp
import snowflake.connector
import os
import re
import sys
import subprocess
from jobs.utils.ReadConfig import ReadConfig
from jobs.utils.sparkSession import spark
import jobs.utils.SP_Caller as Snowprocedure_executor
import jobs.utils.snowflake_connector as Snowflake
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from pyspark import SparkContext
from datetime import datetime
import smtplib, ssl
from smtplib import SMTPException
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from pyspark.sql import HiveContext
sc=spark._sc
sqlContext=HiveContext(sc)


env=os.environ['Env']
config_param='SNOWFLAKE_'+env
config = ReadConfig.read_config()
sfURL=config.get(config_param, 'sfURL')
sfUser=config.get(config_param, 'sfUser')
sfDatabase=config.get(config_param, 'sfDatabase')
sfSchema=config.get(config_param, 'sfSchema')
sfRole=config.get(config_param, 'sfRole')
sfWarehouse=config.get(config_param, 'sfWarehouse')
snowflake_key_path=config.get(config_param, 'snowflake_key_path')
api_link=config.get(config_param, 'api_link')
SNOWFLAKE_SOURCE_NAME =config.get(config_param, 'SNOWFLAKE_SOURCE_NAME')
snowflake_password_for_decoding=config.get(config_param, 'snowflake_password_for_decoding')
sfaccount=config.get(config_param, 'sfaccount')
landing_database=config.get(config_param, 'landing_database')


def sendemail(First_Column,Last_Column,sender, receivers, message,Ingest_Job_ID):
    """
        send email

        Args:
            First_Column: Specify the parameters which want to send as tabular format
            Last_Column: Corresponding values
            sender: Mail ID of the person who will send the mail
            receivers: String with comma seperated mail ids of the receivers
            message: body part of the mail
            Ingest_Job_ID: Unique ID of the Ingestion Process

        """
    print("First Column :",First_Column)
    print("Last Column :",Last_Column)
    df = pd.DataFrame(list(zip(First_Column, Last_Column)), columns=["Name", "Value"])
    msg = MIMEMultipart("alternative")
    receivers=receivers.split(",")
    print(receivers)
    port = 25
    smtp_server= 'higmx.thehartford.com'
    message = str(message)
    html = """\
    <html>
      <head></head>
      <body>
    	<p>{0}<p>
        {1}
        <footer>
            <p>Explore more about this Ingestion Framework here : https://github.thehartford.com/HIG/edo_ops_frameworks/wiki<br><br>
            <p><br>
            <p>This is an automatically generated email - please do not reply to it<br>
        </footer>
      </body>
    </html>
    """.format(message, df.to_html(index=False))
    msg['Subject'] = "Ingestion Job Status in {} env [Using New Pipeline]- ".format(env.upper())+Ingest_Job_ID
    part2 = MIMEText(html, 'html')
    msg.attach(part2)
    try:
        server = smtplib.SMTP(smtp_server, port)
        server.sendmail(sender, receivers, msg.as_string())
        print('Email sent Succesfully.')
    except Exception as e:
        print('Error in sending email: '+str(e))




def sendemail_without_table(sender,receivers,subject,message):
    """
        send email

        Args:
            sender: Mail ID of the person who will send the mail
            receivers: List of String (List elements are Receiver Mail Address)
            message: body part of the mail
            Ingest_Job_ID: Unique ID of the Ingestion Process

        """
    message = str(message)
    footer_part = """Explore more about this Ingestion Fraemwork here : https://github.thehartford.com/HIG/edo_ops_frameworks/wiki  \n \n
This is an automatically generated email - please do not reply to it"""
    body=message+'\n'+'\n'+footer_part
    msg = MIMEText(body)
    print(receivers)
    port = 25
    smtp_server= 'higmx.thehartford.com'
    msg['Subject'] = subject
    try:
        server = smtplib.SMTP(smtp_server, port)
        server.sendmail(sender, receivers, msg.as_string())
        print('Email sent Succesfully.')
    except Exception as e:
        print('Error in sending email: '+str(e))

def file_existance_check(path):
    """
	This function is used to check whether the path exists or not in hdfs
    """
    proc = subprocess.Popen(['hadoop', 'fs', '-test', '-e', path])
    proc.communicate()
    existance_check=proc.returncode
    if existance_check != 0:
        print('{} does not exist'.format(path))
    else :
        print('{}  exist'.format(path))
    return existance_check







def count_check_source(source_connection_info,where_condition):
    """

    source_connection_info: List containing username , password, connection-driver , url, dbTable
    where_condition: Where condition in the count check query
    :return: Count of records in source

    """
    user_name=source_connection_info[0]
    print("User Name for the source : ",user_name)
    password=source_connection_info[1]
    driver=source_connection_info[2]
    print("Driver Name : ",driver)
    url=source_connection_info[3]
    print("JDBC Driver : ",url)
    dbTable=source_connection_info[4]
    print("Table : ",dbTable)
    query=""
    if(where_condition=='None'):
        query="(select count(*) as CHECK_COUNT from "+dbTable+")"
    else:
        query="(select count(*) as CHECK_COUNT from "+dbTable+" where "+where_condition+")"
    able_to_connect=0
    print("Query to be executed to take Source Count : ",query)
    df=0
    try:
        if(driver=='oracle.jdbc.OracleDriver'):
            df = spark.read.format('jdbc') \
                .option('url', url) \
                .option('driver', driver) \
                .option('dbtable', query) \
                .option('user', user_name) \
                .option('password', password) \
                .load()
        elif(driver=='com.microsoft.sqlserver.jdbc.SQLServerDriver'):
            df = spark.read.format('jdbc') \
                .option('url', url) \
                .option('driver', driver) \
                .option('query', query) \
                .option('user', user_name) \
                .option('password', password) \
                .load()
        able_to_connect=0
        print("Source Count : ",int(df.collect()[0].asDict()['CHECK_COUNT']))
        return query, int(df.collect()[0].asDict()['CHECK_COUNT']),able_to_connect
    except Exception as e:
        error_message=str(e)
        able_to_connect = 1
        return error_message,0,able_to_connect

def count_check_hive(hive_table_name):
    print("Hive Table Count Check going on...")
    query = "select count(*) from " + hive_table_name
    destination_count=0
    try:
        ms = spark.sql(query)
        destination_count=int(ms.collect()[0].asDict()['count(1)'])
    except:
        print("The count check in hive is not successful...")
    return query,destination_count

def absolute(x):
    if(x>=0):
        return x
    else:
        return -x






def last_import_date_extractor(column_query,native_hive_table_name,where_clause):
    """
    This function is used to get the last_import_date or last_import_value from hive
    :param column_query:
    :param native_hive_table_name:
    :param where_clause:
    :return:comparison_query
    """
    last_import_date_query="select {} as last_import_date from {}".format(column_query,native_hive_table_name)
    print("Last Import Date Extractor Query : ",last_import_date_query)
    comparison_query=""
    dt=native_hive_table_name.split(".")
    dbname=dt[0]
    table_name=dt[1]
    usedb="USE {}".format(dbname)
    print("USE DB Query :",usedb)
    try:
        spark.sql(usedb)
    except Exception as prob:
        print("The problem appeared while executing '{}' query : ".format(usedb), str(prob))
    existance_query="SHOW TABLES LIKE '"+table_name+"'"
    print(existance_query)
    last_import_date=""
    if(spark.sql(existance_query).count()==0):
        print("No Comaprison Query as this is the First Time")
        return comparison_query
    else:
        try:
            ms = spark.sql(last_import_date_query)
            last_import_date="'"+str(ms.collect()[0].asDict()['last_import_date'])+"'"
        except Exception as disp_except:
            print("Problem in extracting Last Import Date!!!")
            print(disp_except)
        comparison_query=where_clause+last_import_date
        print("Comaprison Query : ",comparison_query)
        return comparison_query


def external_table_partition_dropper(native_table_name,partiton_file_path,value):
    """External Table partition drop is 2 step process --
        Step 1: drop partition form table
        Step 2:remove the file from s3 location"""
    dt=native_table_name.split(".")
    dbname=dt[0]
    table_name=dt[1]
    usedb="USE {}".format(dbname)
    print("USE DB Query :",usedb)
    try:
        spark.sql(usedb)
    except Exception as prob:
        print("The problem appeared while executing '{}' query : ".format(usedb), str(prob))
    existance_query="SHOW TABLES LIKE '"+table_name+"'"
    print(existance_query)
    if(spark.sql(existance_query).count()==0):
        print("The table does not exists , this is first time load")
        return 0,"The table does not exists , this is first time load"
    else:
        query="SHOW PARTITIONS {} partition (run_date='{}')".format(native_table_name,value)
        print("Check partition is available or not for the same day : ",query)
        faliure_indictaor=0
        logs_data=""
        partition_presence_check=spark.sql(query).count()
        check_hdfs_file_location=file_existance_check(partiton_file_path)
        if(partition_presence_check==1):
            print("Already partition exisits with same run date , so deleting the partition")
            query_to_drop_partition="hive -e '"+"ALTER TABLE {} DROP IF EXISTS PARTITION(run_date ='{}')".format(native_table_name,value)+"'"
            logs_data=logs_data+"""Already partition exisits with same run date , so deleting the partition from external hive table"""+'\n'+query_to_drop_partition+'\n'
            print("Query to drop the partition : ",query_to_drop_partition)
            status_to_remove_partition_from_hive_table=os.system(query_to_drop_partition)
            if(status_to_remove_partition_from_hive_table==0):
                print("Partition is successfully dropped having same run_date from the hive table")
                logs_data=logs_data+"Partition is successfully dropped having same run_date from the hive table"+'\n'
            else:
                print("Problem in deleting partition from the hive table")
                logs_data=logs_data+"Problem in deleting partition from external hive table"+'\n'
                faliure_indictaor=1
        if(partition_presence_check==0):
            message="Partition for {} does not exists in the external hive table".format(value)
            print(message)
            logs_data=logs_data+message+'\n'
        if(check_hdfs_file_location==0):
            print("hdfs location exists , have to delete that")
            remove_a_prtition_from_hdfs_location="hadoop fs -rm -r {}".format(partiton_file_path)
            print("Script to remove partition data from hdfs location : ",remove_a_prtition_from_hdfs_location)
            logs_data=logs_data+"Removing partition data from hdfs location : "+'\n' + remove_a_prtition_from_hdfs_location+ '\n'
            status_to_remove_partitoned_file_from_s3_location=os.system(remove_a_prtition_from_hdfs_location)
            if(status_to_remove_partitoned_file_from_s3_location==0):
                print("hdfs location is successfully dropped")
                logs_data=logs_data+"hdfs location is successfully dropped"+'\n'
            else:
                print("Problem in deleting hdfs location")
                logs_data=logs_data+"Problem in deleting hdfs location"+'\n'
                faliure_indictaor=1
        if(check_hdfs_file_location!=0):
            message="The hdfs location {} does not exist".format(partiton_file_path)
            print(message)
            logs_data=logs_data+message+'\n'
        if(partition_presence_check==0 and check_hdfs_file_location!=0):
            faliure_indictaor=0
            run_date=str(datetime.now().strftime("%Y:%m:%d"))
            logs_data="The job is running first time on {}".format(run_date)
            print(logs_data)
        return faliure_indictaor,logs_data


def partition_table_code(original_insert_query):
    #if PARTITION_REQUIRED is enables by user then create table statement has to be below--
    splitter=original_insert_query.split("stored as")
    query=splitter[0]+" partitioned by(run_date string) stored as"+splitter[1]
    print("query to create partitioned table : ",query)
    return query

def insert_query_modifier_for_partion(insert_query,run_date):
    insert_query_parition=insert_query.split("select")
    insert_query_for_rundate_partiton=insert_query_parition[0]+" partition(run_date='"+run_date+"') "+"select "+insert_query_parition[1]
    print("Insert query for paritition : ",insert_query_for_rundate_partiton)
    return insert_query_for_rundate_partiton


def snowflake_external_table_creation(ingest_job_id,SNOW_FLAKE_EXT_TBL_BUILD_IND,SNOW_FLAKE_EXT_TBL_SCH_NM):
    """
    This function is used to call the stored procedure which will create external snowflake table if in the JOB Directory location
    any stage is present
    :param SNOW_FLAKE_EXT_TBL_BUILD_IND: If 'Y' then it will opt for creating snowflake table , else no
    :param SNOW_FLAKE_EXT_TBL_SCH_NM: Schema name where external table will be created
    """
    log_capture=""
    if(SNOW_FLAKE_EXT_TBL_BUILD_IND=='Y' and SNOW_FLAKE_EXT_TBL_SCH_NM!='None'):
        ctx = Snowflake.get_snowflake_connector(snowflake_key_path, snowflake_password_for_decoding, sfUser, sfaccount,
                                                sfWarehouse, sfDatabase, sfSchema, sfRole)
        cs = ctx.cursor()
        query = "CALL APP_BIENT_SHR.SP_OPS_BI_BUILD_SF_EXT_TBL({})".format(ingest_job_id)
        message="Executing the query to create external Snowflake Table : "
        log_capture=log_capture+message+'\n'+query+'\n'
        print(message , query)
        try:
            cs.execute(query)
        except Exception as e:
            print("Exception occured while creating External Table in Snowflake : ", str(e))
            log_capture = log_capture  + str(e) +'/n'
        cs.close()
        ctx.close()

    elif(SNOW_FLAKE_EXT_TBL_BUILD_IND!='Y'):
        log_capture=log_capture+"As the SNOW_FLAKE_EXT_TBL_BUILD_IND is not set to 'Y' , so not attempting to create the external table"+'/n'
        print("As the SNOW_FLAKE_EXT_TBL_BUILD_IND is not to 'Y' , so not attempting to create the external table")

    elif(SNOW_FLAKE_EXT_TBL_BUILD_IND=='Y' and SNOW_FLAKE_EXT_TBL_SCH_NM=='None'):
        log_capture = log_capture +"Although the SNOW_FLAKE_EXT_TBL_BUILD_IND is set to 'Y' but , as the Schema Name is not mentioned , so not creating any External Snowflake Table"
        print("Although the SNOW_FLAKE_EXT_TBL_BUILD_IND is set to 'Y' but , as the Schema Name is not mentioned , so not creating any External Snowflake Table")
    return log_capture

def runner(P_SRC_TBL_SCH_NM,P_SRC_TBL_NM,P_TGT_HIVE_TBL_SCH,P_TGT_HIVE_TBL_NM):
    any_stage_error_indicator=0
    print("Extracting the Sqoop Script from ")
    ctx=Snowflake.get_snowflake_connector(snowflake_key_path,snowflake_password_for_decoding,sfUser, sfaccount, sfWarehouse, sfDatabase, sfSchema, sfRole)
    cs = ctx.cursor()
    meta_logs="Extracting the Job Config from Snowflake OPS_BI_INGEST_JOB_CONFIG table using the below query:" + '\n'
    job_config_extractor = "select * from table ({}.APP_BIENT_SHR.SF_GET_OPS_BI_JOB_CONFIG ('".format(sfDatabase)+P_SRC_TBL_SCH_NM+"','"+P_SRC_TBL_NM+"','"+P_TGT_HIVE_TBL_SCH+"','"+P_TGT_HIVE_TBL_NM+"'))"
    print("Query to extract Job Configurations from Snowflake Table: ",job_config_extractor)

    meta_logs=meta_logs+job_config_extractor+'\n'

    try:
        cs.execute(job_config_extractor)
    except Exception as e:
        print("Problem in executing the Job Config Extractor!!!")
        meta_logs=meta_logs+"Problem in executing the Job Config Extractor!!!"+'\n'+str(e)+'\n'+'\n'

    sqoop_config=[]

    """ 
    If given Source DB, Source Table , Target DB , Target Table does not exists , then the job should not run , 
    in that case list(map(lambda x : str(x),cs.fetchall()[0])) will throw error
    """

    try:
        sqoop_config = list(map(lambda x : str(x),cs.fetchall()[0]))
    except :
        print("No Such Config Presnet!!!")
        print("Length of extracted configs : " , len(sqoop_config))


    if(len(sqoop_config)!=0): #Start further processing only if given Source DB, Source Table , Target DB , Target Table exists & Valid Indicator is Y
        if(sqoop_config[43]=="Y"): #Check for Valid Indicator
            meta_logs=meta_logs+"Started decrypting the Password for Source using the below query:" +'\n'
            decodepquery="SELECT {}.APP_BIENT_SHR.SF_ENCODE_DECODE_STRING('".format(sfDatabase)+sqoop_config[5]+"', 'D')"
            meta_logs=meta_logs+decodepquery+'\n'
            password=""
            try:
                password=str(cs.execute(decodepquery).fetchone()[0])
            except Exception as exap: #If unable to decrypt the password , then it won't able to connect with source using Spark & as a result sqoop won't be executed
                print("Problem in decrypting Password!!!")
                meta_logs=meta_logs+"Problem in decrypting Password!!!"+'\n'+str(exap)+'\n'
            print("The Inserted Sqoop Configuration: ",(sqoop_config))
            concated_version=','.join(map(str,sqoop_config))
            meta_logs=meta_logs+"The Inserted Sqoop Configuration: "+'\n'+'\n'+concated_version+'\n'+'\n'
            meta_logs=meta_logs+"Extracting the batch ID: "+'\n'
            batch_id_extractor="SELECT {}.APP_BIENT_SHR.SP_OPS_BI_GET_CURR_BATCHID ('".format(sfDatabase)+sqoop_config[1]+"','"+sqoop_config[2]+"')"
            meta_logs=meta_logs+batch_id_extractor+'\n'
            print("Executing Batch ID Extractor : ",batch_id_extractor)
            batch_id=str(cs.execute(batch_id_extractor).fetchone()[0])
            print("The batch id is : ",batch_id)
            meta_logs = meta_logs + batch_id+'\n'+'\n'
            ingest_job_id = sqoop_config[0]
            print("Ingest Job ID (Unique Identifier for the Ingestion Job) : ",ingest_job_id)
            print("Log file path : ",sqoop_config[36])
            LOG_FILE_PATH = sqoop_config[36].split('.')[0]
            output_location = LOG_FILE_PATH + '.out'
            print("Output of Sqoop will be stored in the location : ", output_location)
            error_log_location = LOG_FILE_PATH + 'errorlog.out'
            print("Error Logs of Sqoop will be stored in the location : ", error_log_location)
            raw_select_query = sqoop_config[39].upper()
            print("Raw select query used to read data from RDBMS (without adding where clause or any incremental condition) : ",raw_select_query)
            find_index = raw_select_query.find(" FROM ")
            select_query = raw_select_query[0:find_index] + ',' + str(batch_id) + ' as INGEST_BATCH_ID' + raw_select_query[find_index:] #adding batch id as extra column
            print("Select Query after adding the Current Ingest Batch ID : ",select_query)
            dbTable = sqoop_config[11] + '.' + sqoop_config[9]
            print("RDBMS Database Table : ",dbTable)
            incremental = sqoop_config[19]
            print("Incremental Index :", incremental)
            where_condition=sqoop_config[17]
            print("Where Condition to be applied always while importing data from RDMS to hdfs : ", where_condition)
            abc_framecheck_required_or_not=sqoop_config[18]
            print("Count Check Validation after Ingestion required or not : ",abc_framecheck_required_or_not)

            if(sqoop_config[27].lower().endswith(sqoop_config[25].lower())==False):
                sqoop_config[27]=sqoop_config[27]+'/'+sqoop_config[25]
            print("Native directory location : ", sqoop_config[27])

            landing_area_location = sqoop_config[29]
            if (landing_area_location.lower().endswith(sqoop_config[25].lower()) == False):
                landing_area_location = landing_area_location + '/' + sqoop_config[25]
            print("Landing Area Location : ", landing_area_location)

            meta_logs = meta_logs +"The Landing Location for this Ingestion Process : "
            meta_logs = meta_logs + landing_area_location + '\n'

            print("Landing Database", landing_database)

            print("Target hive table name : ",sqoop_config[25])
            landing_table_creation = '''CREATE EXTERNAL TABLE IF NOT EXISTS ''' + landing_database + '.' + sqoop_config[25] + \
                                     " (" + sqoop_config[34].lower() + ",ingest_batch_id int" + ") ROW FORMAT DELIMITED " + \
                                     'FIELDS TERMINATED BY "' + sqoop_config[12] + \
                                     '''" LINES TERMINATED BY "\n"''' + ' LOCATION "' + \
                                     landing_area_location + '''" TBLPROPERTIES("serialization.null.format"="null")'''



            file_format = sqoop_config[31]
            print("The file format for the native layer external table : ", file_format)

            main_table_creation_code = '''CREATE EXTERNAL TABLE IF NOT EXISTS ''' + sqoop_config[28] + '.' + \
                                       sqoop_config[25] \
                                       + " (" + sqoop_config[34].lower() + ",ingest_batch_id int" + ") stored as " \
                                       + file_format + ' LOCATION "' + \
                                       sqoop_config[27] + '''" TBLPROPERTIES("serialization.null.format"="")'''


            message_string = "Starting Data Ingestion from landing table to main table in " + file_format + " format using the below query"

            insert_data_from_landing_table_to_main_table = "insert into " + sqoop_config[28] + '.' + sqoop_config[25] + \
                                                           " select * from " + landing_database + '.' + sqoop_config[25] #insert data from landing table to native table


            partition_required_or_not = sqoop_config[23]
            print("partition required or not : ", partition_required_or_not)

            user_name = sqoop_config[4]
            driver = sqoop_config[6]
            url = sqoop_config[8]
            source_count_check_list = [user_name, password, driver, url, dbTable]

            print("Taking Source count..")
            meta_logs = meta_logs + "Taking Source Count :" + '\n'

            query, source_count, able_to_connect = count_check_source(source_count_check_list, where_condition)

            source_count_check_list=(query, source_count, able_to_connect,"dummy",dbTable)

            meta_logs = meta_logs + query + '\n' + str(source_count) + '\n'

            if (partition_required_or_not == '1'):
                run_date = datetime.now().strftime("%Y%m%d")
                print("Run Date : ", run_date)
                partiton_file_path = sqoop_config[27] + "/run_date=" + run_date #Partition File Location absed on run date
                print("Partition where new data will be loaded : ", partiton_file_path)
                faliure_indictaor,log_captured=external_table_partition_dropper(sqoop_config[28] + '.' + sqoop_config[25], partiton_file_path,run_date)
                any_stage_error_indicator=faliure_indictaor
                meta_logs=meta_logs+log_captured  + '\n'
                main_table_creation_code = partition_table_code(main_table_creation_code)
                insert_data_from_landing_table_to_main_table = insert_query_modifier_for_partion(insert_data_from_landing_table_to_main_table, run_date)
            incremental_condition=""
            """
            While loading data in HDFS , always the landing directory will be overwritten. 
            If the job is not incremental & partitioning is not enabled, then only native directory will be overwritten (truncate & load) .
            If the job is incremental , then no need to drop the native .
            """
            dbname=sqoop_config[28]
            usedb="USE {}".format(dbname)
            spark.sql(usedb)
            existance_query="SHOW TABLES LIKE '"+ sqoop_config[25]+"'"
            print("Query to check Table present or not : ", existance_query)
            count_table=spark.sql(existance_query).count()
            print("Check Table present or not : ", count_table)
            if (incremental == '0' and partition_required_or_not == '0' and count_table!=0):
                meta_logs=meta_logs+"""deleting the native directory as this is not incremental & partitioning  is not enable & this is not the first time ingestion"""+ '\n'
                #deleting the native directory for Full Refresh Jobs-
                print("Specified Native directory : ",sqoop_config[27])
                check_hdfs_file_location=file_existance_check(sqoop_config[27])
                if(check_hdfs_file_location==0 and source_count!=0):
                    execute_value = "hdfs dfs -rm -r " + sqoop_config[27]
                    print("As this is not incremental pull & partitioning is not enabled so deleting the native  directory : ", execute_value)
                    meta_logs=meta_logs+execute_value+'\n'
                    status_op=os.system(execute_value)
                    parent_location_creation_if_not_exists="hdfs dfs -mkdir -p "+sqoop_config[27]
                    print("Creating the parent location back : ", parent_location_creation_if_not_exists)
                    status_op_location_creation=os.system(parent_location_creation_if_not_exists)
                    if(status_op==0 and status_op_location_creation==0):
                        print("The native directory successfully deleted")
                    else:
                        any_stage_error_indicator=1
                        print("Error appeared while clearning the native layer : ")
                        meta_logs=meta_logs+"Error appeared while clearning the native layer , so not procedding with SQOOP Job"
                elif(check_hdfs_file_location==0 and source_count==0):
                    msfrc="""As this is not incremental pull & partitioning is not enabled so the native  
directory should be deleted but as source count is 0 , so not deleting the native directory """
                    meta_logs = meta_logs +msfrc
                    print(msfrc)
            elif (incremental == '0' and partition_required_or_not == '1'):
                print("Native Table directory can not be deleted as partitioning is enabled!!!")
            elif(incremental == '1'):
                print("Incremental Index enable , so the native table will not be deleted")
                incremental_query_in_hive=sqoop_config[20]
                print("Query to be applied in Hive Incremental Column : ", incremental_query_in_hive)
                where_condition_on_RDBMS=sqoop_config[21]
                print("Where Condition to be applied in RDBMS to filter out only incremental data : ",where_condition_on_RDBMS)
                comparison_query = last_import_date_extractor(incremental_query_in_hive, sqoop_config[28] + '.' + sqoop_config[25],where_condition_on_RDBMS) #extract the condition to be applied while performing incremental import
                if (comparison_query != ""):
                    print("This is Incremental Load (for second day or more)")
                    incremental_condition =comparison_query
                    print("The incremental condition to be applied : ",incremental_condition)
                    if (sqoop_config[17] != 'None'):
                        print("Adding default WHERE_CONDITION  with Incremental load Condition")
                        sqoop_config[17] = sqoop_config[17] + " AND " + incremental_condition
                        print("Overall condition to import data : ",sqoop_config[17])
                    else:
                        print("There is not where_condition explicitly specified , only incremental condition will be applied..")
                        sqoop_config[17] = comparison_query
                else:
                    print("First time load so there is no difference between Full Load & Incremental Load , so no extra condition!!!")

            if(sqoop_config[17]!='None'):
                select_query=select_query.upper()
                select_query=select_query.split("WHERE")[0]
                condition=" WHERE "+sqoop_config[17]+" AND "+"\\$CONDITIONS"
                select_query=select_query+condition
                print("Select query after applying all conditions : ",select_query)

            if (sqoop_config[3].upper() == 'ORACLE'):
                sqoop_code = '''sqoop import \
                -Dmapreduce.job.user.classpath.first=true \
                -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
                -Dmapreduce.task.timeout=0 \
                --connect "''' + sqoop_config[8] + \
                 '''" --username=''' + sqoop_config[4] + \
                 ''' --password=''' +  "'" + password +  "'" + \
                 ''' --query "''' + select_query + \
                 '" --fields-terminated-by "' + sqoop_config[12] + \
                 '''" --delete-target-dir \
                 --target-dir ''' + landing_area_location + \
                 ''' --split-by "''' + sqoop_config[14] + \
                 '''" --num-mappers ''' + sqoop_config[13] + " 1>" + output_location + " 2>" + error_log_location
            else:
                sqoop_code = '''sqoop import \
                    -Dmapreduce.job.user.classpath.first=true \
                    -Dorg.apache.sqoop.splitter.allow_text_splitter=true \
                    -Dmapreduce.task.timeout=0 \
                    --connect "''' + sqoop_config[8] + \
                     '''" --username=''' + sqoop_config[4] + \
                     ''' --password=''' +  "'" + password +  "'" + \
                     ''' --driver=''' + sqoop_config[6] + \
                     ''' --query "''' + select_query + \
                     '" --fields-terminated-by "' + sqoop_config[12] + \
                     '''" --delete-target-dir \
                     --target-dir ''' + landing_area_location  + \
                     ''' --split-by "''' + sqoop_config[14] + \
                     '''" --num-mappers ''' + sqoop_config[13] + " 1>" + output_location + " 2>" + error_log_location



            
            hive_table_name = sqoop_config[28] + '.' + sqoop_config[25]
            display_password = '*' * len(password)
            display_sqoop_code = sqoop_code.replace(password, display_password).replace("$", "\$")
            abc_threshold = sqoop_config[40]
            print("ABC Threshold Percentage : ",abc_threshold)
            send_mail_receiver = sqoop_config[37]
            print("Notification Receiver : ",send_mail_receiver)
            SNOW_FLAKE_EXT_TBL_BUILD_IND=sqoop_config[41]
            print("Required to build Snowflake External Table or Not : ",SNOW_FLAKE_EXT_TBL_BUILD_IND)
            SNOW_FLAKE_EXT_TBL_SCH_NM=sqoop_config[42]
            print("Schema name where external table will be created : ",SNOW_FLAKE_EXT_TBL_SCH_NM)
            return meta_logs,ingest_job_id, send_mail_receiver, message_string, main_table_creation_code, insert_data_from_landing_table_to_main_table, str(
                landing_table_creation), str(sqoop_code), error_log_location, output_location, sqoop_config[0], \
                   batch_id, source_count_check_list, hive_table_name, display_sqoop_code, abc_threshold,where_condition,abc_framecheck_required_or_not,any_stage_error_indicator,SNOW_FLAKE_EXT_TBL_BUILD_IND,SNOW_FLAKE_EXT_TBL_SCH_NM,partition_required_or_not
        elif (sqoop_config[43] != "Y"):
            # Job should not run now as Vlaid Indicator is not set
            print("The Job can not be executed as Valid Indicator is not set to Y")
            return "","", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "","","",""
    elif(len(sqoop_config)==0):
        #Wrong Command Line Arguments passed
        print("The Job can not be executed as no such source DB , source Table , Target DB , Target Table combination exists")
        return "","","","","","","","","","","","","","","","","","","","","",""
    cs.close()
    ctx.close()



def main():
    print("Inside Main Function")
    source_userId = sys.argv[1] #Source Table Schema Name
    source_tablename = sys.argv[2] #Source Table Name
    target_db=sys.argv[3] #Target Database Name
    target_table=sys.argv[4] #Target Table Name
    meta_logs,ingest_job_id,send_mail_receiver,message_string,main_table_creation_code,insert_data_from_landing_table_to_main_table,landing_table_creation_code,data_loader_code,error_log_location,output_location,job_id,batch_id,source_count_check_list,hive_table_name,display_sqoop_code,abc_threshold,where_condition,abc_framecheck_required_or_not,any_stage_error_indicator,SNOW_FLAKE_EXT_TBL_BUILD_IND,SNOW_FLAKE_EXT_TBL_SCH_NM,partition_required_or_not=runner(source_userId, source_tablename, target_db, target_table)
    print("Ingest Job ID : ",ingest_job_id)
    print("Batch ID : ",batch_id)
    print("Error or exception before running sqoop code : ",any_stage_error_indicator)
    if(len(meta_logs)!=0):
        #Correct Command Line Arguments given (such source , destination combination exists in Job Config Table) and Valid Indicator is set
        Snowprocedure_executor.sprunner("{}.APP_BIENT_SHR.SP_OPS_BI_OPEN_BATCH_JOB_PRCSS_STTS".format(sfDatabase), [batch_id, job_id])
        if(any_stage_error_indicator==1):
            print("Before starting the SQOOP Job , error appeared , so not procedding further")
            meta_logs=meta_logs.replace("'","''").replace("\\","\\\\")
            Snowprocedure_executor.sprunner("{}.APP_BIENT_SHR.SP_OPS_BI_CLOSE_BATCH_JOB_PRCSS_STTS".format(sfDatabase),[batch_id, job_id, "'" + 'E' + "'", "TO_VARIANT('" + meta_logs + "')", 0,0])
            sender = "OpsBIChargers@thehartford.com"
            receivers = send_mail_receiver
            subject="Ingestion Job Status in {} env [Using New Pipeline]- ".format(env.upper())+ingest_job_id
            message_warning="An excpetion occured before running the SQOOP Job , check the Process Status table for details"
            print(message_warning)
            sendemail_without_table(sender,receivers.split(","),subject,message_warning)
            print("$exit_code$=1")  # To indicate something wrong happened
        else:
            query,source_count,able_to_connect,dummy_data,dbTable=source_count_check_list
            print("Able to Connect or Not",able_to_connect)
            if(able_to_connect==0 and source_count!=0):
                #able to connect with source with user id , decrypted password & JDBC URL
                meta_logs=meta_logs+query+'\n'+str(source_count)+'\n'
                print("Loading the data from RDBMS to HDFS Landing location..")
                print("The Sqoop Code to load the data from RDBMS to landing directory: ", display_sqoop_code)
                status = 0
                status = os.system(data_loader_code)
                sf_status_update = ""
                if(status==0):
                    print("The transfer of data from source to hdfs exited without errors!!!")
                    sf_status_update="C"
                else:
                    print("Sqoop Job is not successful!!!")
                    sf_status_update="E" #As the Sqoop failed so making status as E
                logs=""
                logs = logs + "Sqoop Code to load data from RDBMS to HDFS Landing Directory : " + '\n'
                logs = logs + display_sqoop_code + '\n'
                try:
                    with open(output_location, 'r') as file:
                        logs = logs + file.read()
                except Exception as outrd:
                    logs = logs + "Problem in reading the logs from {} file".format(output_location) + '\n' + str(outrd)
                try:
                    with open(error_log_location, 'r') as file:
                        logs = logs + file.read()
                except Exception as errrd:
                    logs = logs + "Problem in reading the logs from {} file".format(error_log_location) + '\n' + str(errrd)
                if(status==0):
                    # As SQOOP job is successful , so procedding with Landing & Native table creation
                    logs=logs+'\n'+"Executing Landing Table Creation Code :" +'\n' +landing_table_creation_code
                    print("Creating Landing table.."),
                    print("Landing Table Creation Code: ", landing_table_creation_code)
                    try:
                        spark.sql(landing_table_creation_code)
                    except Exception as x:
                        logs=logs+'\n'+"Problem in creating the Landing Table!!!"+'\n'+str(x)
                        sf_status_update="E"
                    logs = logs + '\n' + "Executing Main Table Creation Code :" + '\n' + main_table_creation_code
                    print("Creating Target table (Main Table in Native layer)..")
                    print("Main Table Creation Code: ", main_table_creation_code)
                    try:
                        spark.sql(main_table_creation_code)
                    except Exception as y:
                        logs = logs + '\n' + "Problem in creating the Native Table!!!" + '\n' + str(y)
                        sf_status_update = "E"
                    logs = logs + '\n' +message_string+'\n'+insert_data_from_landing_table_to_main_table
                    print("Inserting data from Landing table to main table..")
                    #query_execute_data_load_main_table = """hive -e '""" + insert_data_from_landing_table_to_main_table + """'"""
                    query_execute_data_load_main_table =insert_data_from_landing_table_to_main_table
                    print("Insert Query to load the data from landing table to main table : ",query_execute_data_load_main_table)
                    print("Partition required or not : ",partition_required_or_not)
                    try:
                        if(partition_required_or_not=='0'):
                        #os.system(query_execute_data_load_main_table)
                            spark.sql(query_execute_data_load_main_table)
                        else:
                            spark.sql(query_execute_data_load_main_table)
                            repair_table_code= "msck repair table  {}".format(hive_table_name)
                            print("Repairing the table for new partitons: ",repair_table_code)
                            spark.sql(repair_table_code)
                    except Exception as z:
                        logs = logs + '\n' + "Problem in Inserting data from Landing Table to Native Table!!!" + '\n' + str(z)
                        sf_status_update = "E"
                    if(sf_status_update != "E"):
                        captured_logs=snowflake_external_table_creation(ingest_job_id, SNOW_FLAKE_EXT_TBL_BUILD_IND,SNOW_FLAKE_EXT_TBL_SCH_NM)
                        logs=logs+'\n'+captured_logs
                else:
                    error_message="As the SQOOP job failed so that not procedding with further landing & native hive table creation!!!"
                    logs=logs+'\n'+error_message
                target_count=0
                if(abc_framecheck_required_or_not=='1' and sf_status_update!="E"):
                    #If whole sqoop ingestion including landing & native table creation is successful & abc framework is enabled , then only go for source destination count check i.e. validation
                    logs=logs+'\n'+"Taking Target Counts:"+'\n'
                    query,target_count=count_check_hive(hive_table_name)
                    logs=logs+query+'\n'+str(target_count)+'\n'
                    print("Only for stats")
                    print("SF Status: ",sf_status_update)
                    print((source_count!=target_count))
                    print("ABC threshold",abc_threshold)
                    print("Source Count",source_count)
                    print("Target Count",target_count)
                    print("Source Count , Target Count Difference :",absolute(source_count - target_count))
                    print("The Count difference between Source & Destination is :",(float(absolute(source_count - target_count)) / float(source_count)) * 100)
                    print("Count difference Condition Satisfied or Not :",(float(absolute(source_count - target_count)) / float(source_count)) * 100 > float(abc_threshold))
                    #Possibility of Warning state will come only if the abc frame work is enabled!!!
                    if((source_count!=target_count) and ((float(absolute(source_count - target_count)) / float(source_count)) * 100 > float(abc_threshold)) and (sf_status_update!='E')):
                        sf_status_update="W"
                print("Snowflake Status", sf_status_update)
                print("All computation completed , updating Audit & Recon...")
                logs=meta_logs+'\n'+logs
                logs=logs.replace("'","''").replace("\\","\\\\")
                if(abc_framecheck_required_or_not=='0'):
                    Snowprocedure_executor.sprunner("{}.APP_BIENT_SHR.SP_OPS_BI_CLOSE_BATCH_JOB_PRCSS_STTS".format(sfDatabase),[batch_id,job_id,"'"+sf_status_update+"'","TO_VARIANT('"+logs+"')",0,0])
                else:
                    Snowprocedure_executor.sprunner("{}.APP_BIENT_SHR.SP_OPS_BI_CLOSE_BATCH_JOB_PRCSS_STTS".format(sfDatabase),[batch_id, job_id, "'" + sf_status_update + "'", "TO_VARIANT('" + logs + "')", source_count,target_count])
                message=""
                if(sf_status_update=='C'):
                    message='''Hello Team the ingestion job is successfully completed! Please check the Process_Status Table [APP_BIENT_SHR.OPS_BI_BTCH_PROCESS_STATUS] for details'''
                elif(sf_status_update=='W'):
                    message='''Hello Team the ingestion job is completed! Primary Information attached below. Please check the Process_Status Table [APP_BIENT_SHR.OPS_BI_BTCH_PROCESS_STATUS] for more details(use Ingest Job ID for Search)'''
                else:
                    message='''Hello Team the ingestion job failed! Please check the Process_Status Table [APP_BIENT_SHR.OPS_BI_BTCH_PROCESS_STATUS] for details'''
                sender="OpsBIIngestionFW@thehartford.com"
                receivers=send_mail_receiver
                if(abc_framecheck_required_or_not=='1' and sf_status_update!='E'):
                    #If ABC Framework is enable then only send source & target count via mail
                    First_Column = ["INGEST JOB ID", "SOURCE NAME", "DESTINATION NAME", "SOURCE COUNT", "DESTINATION COUNT"]
                    Last_Column = [ingest_job_id,source_count_check_list[4],hive_table_name,source_count,target_count]
                    sendemail(First_Column, Last_Column, sender, receivers, message, ingest_job_id)
                    print("$exit_code$=0")
                elif(abc_framecheck_required_or_not=='0' and sf_status_update!='E'):
                    # If ABC Framework is not enable then send only Job ID , source & destination name only
                    First_Column = ["INGEST JOB ID", "SOURCE NAME", "DESTINATION NAME"]
                    Last_Column = [ingest_job_id, source_count_check_list[4], hive_table_name]
                    sendemail(First_Column, Last_Column, sender, receivers, message, ingest_job_id)
                    print("$exit_code$=0")
                else:
                    #Something went wrong!!!
                    subject = "Ingestion Job Status in {} env [Using New Pipeline]- ".format(env.upper()) + ingest_job_id
                    sendemail_without_table(sender, receivers.split(","), subject, message)
                    print("$exit_code$=1")  # To indicate something wrong happened
                try:
                    print("Deleting the SQOOP output log directory location")
                    os.remove(output_location)  # Once everything done , remove the Sqoop log directory as logs are cpatured in  Snowflake level
                    print("Deleting the SQOOP error log directory location")
                    os.remove(error_log_location)
                except:
                    print("Problem in deleting the Sqoop Log directory!!!")
            elif(able_to_connect==0 and source_count==0):
                #This part will be executed if able to connect with source but the source has no data
                sender = "OpsBIIngestionFW@thehartford.com"
                receivers = send_mail_receiver
                subject="Ingestion Job Status in {} env [Using New Pipeline]- ".format(env.upper())+ingest_job_id
                message_warning="The Source Table has no data (refer below query) , so not proceeding with Ingestion Process "+'\n'+'\n'+query
                print(message_warning)
                print([batch_id, job_id,"'E'", "TO_VARIANT('" + query + "')",0, 0])
                Snowprocedure_executor.sprunner("{}.APP_BIENT_SHR.SP_OPS_BI_CLOSE_BATCH_JOB_PRCSS_STTS".format(sfDatabase),[batch_id, job_id,"'E'", "TO_VARIANT('" + message_warning + "')",0, 0])
                sendemail_without_table(sender,receivers.split(","),subject,message_warning)
                print("$exit_code$=1")  # To indicate something wrong happened
            else:
                #This part will be executed if unable to connect with source
                sender = "OpsBIIngestionFW@thehartford.com"
                receivers = send_mail_receiver
                subject="Ingestion Job Status in {} env [Using New Pipeline]- ".format(env.upper())+ingest_job_id
                message_warning="An excpetion occured while connecting with Source , check the below message for more details..."+'\n'+'\n'+query
                print(message_warning)
                print([batch_id, job_id,"'E'", "TO_VARIANT('" + query + "')",0, 0])
                Snowprocedure_executor.sprunner("{}.APP_BIENT_SHR.SP_OPS_BI_CLOSE_BATCH_JOB_PRCSS_STTS".format(sfDatabase),[batch_id, job_id,"'E'", "TO_VARIANT('" + message_warning + "')",0, 0])
                sendemail_without_table(sender,receivers.split(","),subject,message_warning)
                print("$exit_code$=1")  # To indicate something wrong happened
    else:
        #pass if valid indicator is not set or no source DB , source Table , Target DB , Target Table combination exists
        print("$exit_code$=0")
        pass





main()
