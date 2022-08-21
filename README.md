# DAG_IMPORT_CSV_CLOUD_SQL

A Composer DAG to import CSV files from cloud storage to Cloud SQL

## Challenges solved

- **Cloud SQL security concern**: The dag will execute Cloud SQL statement without exposing Cloud SQL public IP, using a local proxy, or exposing database username/password in composer environment.
- **Simultaneous Cloud SQL Import Operations**: This will allow you to run multiple DAGs to import CSV files to the same database instance simultaneously and without clashing with each other.
- **Import multiple files to the same table**: using this DAG you can import multiple CSV files to the same table.
- **Data Validation**: Old data are deleting from the table only after validating the new records have been imported successfully.

## Main Features 

- Import multiple CSV files from storage bucket into the Cloud SQL database
- This script imports the files one by one to CLOUD SQL using GCP import API
- The script has pre/post SQL statements to execute
- Files are queued in a storage bucket
- Files will be processed/imported one by one and moved to "imported folder" after being imported
- Each file will have a post SQL statement to execute after the import process.
- Multiple DAGs can run simultaneously with the same database instance without clashing with each other, the DAGs has a lock/unlock mechanism to prevent more than one process from importing to the same database instance, DAGs will be locked waiting for the running operation to finish  


## Configuration

Change the values of the below class to fit your project

``` python
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
```

## Permissions Setup

- Add compser service account as "Cloud SQL Admin" in SQL project
- Add compser service account as "Cloud Bucket Admin" for the bucket in (CONF.BUCKET)
- Add cloud sql service account as "Bucket Object Viewer" to the bucket in (CONF.BUCKET)

## Contributing

Feel free to enhance the DAG, pull requests are welcome.

## Authors

* **Ahmad Issa** 
