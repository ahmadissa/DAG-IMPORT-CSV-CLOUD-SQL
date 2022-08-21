##################################################
# Import multiple CSV files from storage bucket into the Cloud SQL database
# This script imports the files one by one to CLOUD SQL using GCP import API
# The script has pre/post SQL statements to execute
# Files are queued in a storage bucket
# Files will be processed/imported one by one and moved to "imported folder" after being imported
# Each file will have a post SQL statement to execute after the import process.
# To configure the DAG please check the CONF section
##################################################
# Author: Ahmad Issa
# Version: 0.0.1
# Mmaintainer: Ahmad Issa
# Email: aissa@woolworths.com.au
# Status: testing
##################################################

from airflow import AirflowException
from airflow import DAG
from airflow import models
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from datetime import datetime, timedelta
# from gcs_plugin.operators import gcs_to_gcs
from google.cloud import bigquery
from google.cloud import storage
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials
from airflow.operators.dummy_operator import DummyOperator
import google.cloud.storage
import re


import json
import time

"""
############################################################################################
################################ PERMISSIONS ###############################################
############################################################################################

- Add composer service account as "Cloud SQL Admin" in SQL project
- Add composer service account as "Cloud Bucket Admin" for the bucket in (CONF.BUCKET)
- Add cloud SQL service account as "Bucket Object Viewer" to the bucket in (CONF.BUCKET)

"""


########################################################################################
########################################################################################
# CONFIGURE THE BELOW
########################################################################################
########################################################################################
class CONF:
    CLOUD_SQL_PROJECT = "your_project_id"
    SQL_INSTANCE = "database_instance_name"
    OPERATION_WAIT_TIMEOUT = 30  # minutes  to wait for another running operation
    SCHEDULE_INTERVAL = "@once"
    DAG_NAME = "IMPORT_TO_CLOUDSQL"
    BUCKET = "storage_bucket_name"
    PRE_SQL_QUERYS = [
        "delete from some_table where ACTIVE=0;"]
    POST_SQL_QUERYS = ["delete from some_table where ACTIVE=0;"]
    CSV_FILES_TO_IMPORT = ["file_1.CSV",
                           "file_2.CSV",
                           "file_3.CSV",
                           "file_4.CSV"]
    # excute statement after import each file, number of statement should match number of files
    # put empty string "" incase you want to ignore pre_sql for a file or files
    FILES_POST_SQL_QUERYS = ["SQL STATEMENT 1;",
                             "SQL STATEMENT 2;",
                             "SQL STATEMENT 3;",
                             "SQL STATEMENT 4;"]
    DATABASE_NAME = "database_name"
    DATABASE_TABLE_NAME = "table_name"
    EMAIL_WHEN_FAIL = "xxx@woolworths.com.au"
    EMAIL_ON_FAILURE = True
    EMAIL_ON_RETRY = True
    RETRIES = 0
########################################################################################
########################################################################################
# END OF CONFIGURATION
########################################################################################
########################################################################################


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 12),
    'email': CONF.EMAIL_WHEN_FAIL,
    'email_on_failure': CONF.EMAIL_ON_FAILURE,
    'email_on_retry': CONF.EMAIL_ON_RETRY,
    'retries': CONF.RETRIES,
    'retry_delay': timedelta(minutes=5)
}

FMT = '%Y-%m-%d %H:%M:%S'


def move_file(
    bucket_name, blob_name, destination_bucket_name, destination_blob_name

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
    source_blob.delete()
    print(
        "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )


def move_after_import(filename):
    move_file(CONF.BUCKET, filename, CONF.BUCKET, "imported/"+filename)


def write_to_bucket(path, contents):
    """Given a gs:// path, returns contents of the corresponding blob."""
    client = storage.Client()
    bucket = client.get_bucket(CONF.BUCKET)
    blob = storage.Blob(path, bucket)
    blob.upload_from_string(contents)


def delete_file(path):
    """Given a gs:// path, returns contents of the corresponding blob."""
    client = storage.Client()
    bucket = client.get_bucket(CONF.BUCKET)
    blob = storage.Blob(path, bucket)
    blob.delete()


def sql_exec(sql_query):

    sql_query_file = "data.sql"
    write_to_bucket(sql_query_file, sql_query)

    lock_process(sql_query_file)
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)

    # Project ID of the project that contains the source instance.
    project = CONF.CLOUD_SQL_PROJECT

    # Cloud SQL instance ID. This does not include the project ID.
    instance = CONF.SQL_INSTANCE
    print("sql_exec:excuting:", sql_query)
    instances_import_request_body = {
        "importContext": {
            "kind": "sql#importContext",
            "fileType": "SQL",
            "uri": "gs://"+CONF.BUCKET+"/"+sql_query_file,
            "database": CONF.DATABASE_NAME,
        }
    }

    request = service.instances().import_(project=project, instance=instance,
                                          body=instances_import_request_body)
    response = request.execute()
    print(response)
    executeWaitOperationToFinish(response["name"])
    delete_file(sql_query_file)


# sql_pre_excute: excute pre sql statements
def sql_pre_excute():
    queries = ""
    for query in CONF.PRE_SQL_QUERYS:
        queries = queries+query+"\n"
    sql_exec(queries)


def sql_post_excute():
    queries = ""
    for query in CONF.POST_SQL_QUERYS:
        queries = queries+query+"\n"
    sql_exec(queries)


def executeWaitOperationToFinish(operation_id):
    if operation_id == "":
        AirflowException("empty operation id")
        return False
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)

    # Project ID of the project that contains the instance.
    project = CONF.CLOUD_SQL_PROJECT

    while True:
        request = service.operations().get(project=project, operation=operation_id)
        response = request.execute()
        print(response)
        if response.get("status") == "DONE":
            error = response.get("error")
            if error:
                # Extracting the errors list as string and trimming square braces
                error_msg = str(error.get("errors"))[1:-1]
                raise AirflowException(error_msg)
            # No meaningful info to return from the response in case of success
            return
        time.sleep(5)


def executeImportToCloudSQL(csv_file_uri):
    lock_process(csv_file_uri)
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)

    # Project ID of the project that contains the source instance.
    project = CONF.CLOUD_SQL_PROJECT

    # Cloud SQL instance ID. This does not include the project ID.
    instance = CONF.SQL_INSTANCE

    instances_import_request_body = {
        "importContext": {
            "kind": "sql#importContext",
            "fileType": "CSV",
            "uri": "gs://"+CONF.BUCKET+"/"+csv_file_uri,
            "database": CONF.DATABASE_NAME,
            "csvImportOptions": {
                "table": CONF.DATABASE_TABLE_NAME
            }
        }
    }

    request = service.instances().import_(project=project, instance=instance,
                                          body=instances_import_request_body)
    response = request.execute()
    print(response)
    executeWaitOperationToFinish(response["name"])
    move_after_import(csv_file_uri)


def getTimeNow():
    return datetime.now().strftime(FMT)


def lock_process(importing_filename):
    filename = "lock_"+CONF.CLOUD_SQL_PROJECT+"_"+CONF.SQL_INSTANCE
    try:
        with open(filename, "w") as f:
            print("Locking:"+filename)
            f.truncate(0)
            f.write(getTimeNow()+"||"+importing_filename)
            f.close()
    except Exception as e:
        print("lock_process:error writing to file:" + e)
        raise AirflowException(e)


def unlock_process():
    filename = "lock_"+CONF.CLOUD_SQL_PROJECT+"_"+CONF.SQL_INSTANCE
    try:
        with open(filename, "w") as f:
            print("unLocking:"+filename)
            f.truncate(0)
            f.close()
    except Exception as e:
        print("unlock_process:error writing to file:" + e)
        raise AirflowException(e)


def on_failure_callback(context):
    unlock_process()


def can_lock():
    filename = "lock_"+CONF.CLOUD_SQL_PROJECT+"_"+CONF.SQL_INSTANCE
    try:
        with open(filename) as f:
            fileData = f.read()
            f.close()

            lockedFile = fileData.split("||")
            if len(lockedFile) != 2:
                print("invalid time/file data:" + fileData)
                return True
            datetimeNow = datetime.strptime(getTimeNow(), FMT)
            opLockDatetime = datetime.strptime(lockedFile[0], FMT)
            minutes = (datetimeNow - opLockDatetime).total_seconds() / 60
            if int(minutes) < int(CONF.OPERATION_WAIT_TIMEOUT):
                print("Another Operation running:" + fileData)
                return False

        return True
    except IOError:
        return True


def try_lock_process():

    while can_lock() != True:
        print("Process Locked")
        time.sleep(5)
    # LOCK process with table name
    lock_process(CONF.DATABASE_TABLE_NAME)


with models.DAG(CONF.DAG_NAME, default_args=default_args, schedule_interval=CONF.SCHEDULE_INTERVAL, catchup=False) as dag:

    operators = []
    # Add operators to array, excution will be done sync, one by one

    # task to lock importing data to the same database instance
    operators.append(PythonOperator(
        task_id='tryLockSQLOperations',
        python_callable=try_lock_process,
        on_failure_callback=on_failure_callback,
        dag=dag,
    ))
    # excute pre sql statements
    operators.append(PythonOperator(
        task_id='EXCUTE_PRE_SQL',
        python_callable=sql_pre_excute,
        on_failure_callback=on_failure_callback,
        dag=dag,
    ))
    # import files to the table
    for i in range(len(CONF.CSV_FILES_TO_IMPORT)):
        csv_file_uri = CONF.CSV_FILES_TO_IMPORT[i]
        sql_statement = CONF.FILES_POST_SQL_QUERYS[i]
        operators.append(PythonOperator(
            task_id=re.sub(r'\W+', '_', csv_file_uri),
            python_callable=executeImportToCloudSQL,
            op_kwargs={'csv_file_uri': csv_file_uri},
            on_failure_callback=on_failure_callback,
            dag=dag))
        if sql_statement != "":
            operators.append(PythonOperator(
                task_id="POST_SQL_"+re.sub(r'\W+', '_', csv_file_uri),
                python_callable=sql_exec,
                op_kwargs={'sql_query': sql_statement},
                on_failure_callback=on_failure_callback,
                dag=dag))

    # excute post sql statements
    operators.append(PythonOperator(
        task_id='EXCUTE_POST_SQL',
        python_callable=sql_post_excute,
        on_failure_callback=on_failure_callback,
        dag=dag,
    ))
    # unlock the process
    operators.append(PythonOperator(
        task_id='unlockSQLOperations',
        python_callable=unlock_process,
        dag=dag,
    ))

    for index in range(len(operators)):
        if index > 0:
            operators[index-1] >> operators[index]
