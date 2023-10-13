# from datetime import datetime, timedelta

# # from airflow import DAG

# from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator, GCSCreateBucketOperator, GCSDeleteBucketOperator

from airflow.models import Variable

from airflow.operators.bash import BashOperator
# from airflow.models import TaskInstance
# from airflow.decorators import dag, task

import os
import json

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'oskar'
}

PROJECT_ID = Variable.get("PROJECT_ID")
BUCKET_NAME = Variable.get("BUCKET_NAME")
DATASET_NAME = Variable.get("DATASET_NAME")
TABLE_NAME = Variable.get("TABLE_NAME")
LOGS_FOLDER = Variable.get("LOGS_FOLDER")
LOGS_JSON_FOLDER = Variable.get("LOGS_JSON_FOLDER")

with DAG(
    dag_id='network_packet_logger',
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=1),
    schedule='@hourly',
    catchup=False,
) as dag:
    
    create_bucket = GCSCreateBucketOperator(
        task_id='create_bucket',
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,  # replace with your GCP project ID
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id='delete_bucket',
        bucket_name=BUCKET_NAME,
        force=True,  # this will delete the bucket even if it's not empty
    )

    # could be done using bashoperator for sure
    @task()
    def get_raw_log_files():
        # TODO make sure file is finished. maybe skip the last file?
        files = os.listdir(LOGS_FOLDER)
        files = [os.path.join(LOGS_FOLDER, f) for f in files]
        # keep only files. not folders
        files = [f for f in files if os.path.isfile(f)]
        return files
    
    # should a output folder exist? or just a permanent temp folder somewhere?
    @task()
    def make_output_folders():
        os.makedirs(LOGS_JSON_FOLDER, exist_ok=True)
        os.makedirs(LOGS_JSON_FOLDER, exist_ok=True)

    @task()
    def remove_file(filepath):

        print(f"Removing {filepath}")

        # had to add -f flag. scheduler was stopping due to asking if to remove wirte protected file
        # commented out to keep the task in graph but not remove files for development purposes
        # os.system(f'rm -f {filepath}')

        print(f"Removed {filepath}")

    # https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
    @task()
    def process_raw_file(input_file):

        # file processing makes running the dag concurrently throw errors
        # probably access to the same file by multiple processes

        # TODO convert to bash operator
        output_file = os.path.join(LOGS_JSON_FOLDER, os.path.basename(input_file) + '.json')
        print("creating", output_file, "file from", input_file)

        # TODO unsanitized input. is it a problem?
        os.system(f"tshark -r {input_file} -T json > {output_file}")

        # Read the JSON file
        with open(output_file, 'r') as f:
            logs = json.load(f)

        def parse_log(d: dict) -> dict:

            layers = d.get('_source', {}).get('layers', {})
            ts = layers.get('frame', {}).get('frame.time')
            ts = ts.rsplit(' ', 1)[0]  # Remove the time zone
            ts = ts[:ts.rfind('.') + 7] # only keep 6 fractional digits

            # TODO for some reason cant json.dumps datetime object
            # ts = datetime.strptime(ts, '%b %d, %Y %H:%M:%S.%f')  # Parse the date and time
            
            return {
                'timestamp': ts,
                'ip_src': layers.get('ip', {}).get('ip.src'),
                'ip_dst': layers.get('ip', {}).get('ip.dst'),
                'tcp_srcport': layers.get('tcp', {}).get('tcp.srcport'),
                'tcp_dstport': layers.get('tcp', {}).get('tcp.dstport'),
                'data_len': layers.get('data', {}).get('data.len')
            }

        # Convert JSON objects to newline delimited JSON strings
        logs = [json.dumps(parse_log(log)) for log in logs]
        with open(output_file, 'w') as f:
            f.write('\n'.join(logs))

        return output_file

    @task
    def reduce(values):
        return list(values)

    raw_log_files = get_raw_log_files()

    # TODO does this need to be reduced before uploading files from output folder?
    files_json = process_raw_file.expand(input_file=raw_log_files)
    
    files_json_reduced = reduce(files_json)

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src=files_json_reduced,
        dst='', # '/' creates a folder in bucket
        bucket=BUCKET_NAME
    )
    
    gcs_to_bq = GCSToBigQueryOperator(
        task_id='bucket_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects='logs.json',
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
        destination_project_dataset_table=f"{PROJECT_ID}:{DATASET_NAME}.{TABLE_NAME}"
    )

    # this task is redundant. delete bucket task will delete all objects in bucket
    # empty_bucket = GCSDeleteObjectsOperator(
    #     task_id="empty_bucket",
    #     bucket_name=BUCKET_NAME,
    #     objects='*'
    # )

    make_output_folders() >> files_json >> remove_file.expand(filepath=raw_log_files)

    upload_to_gcs >> remove_file.expand(filepath=files_json)
    create_bucket >> upload_to_gcs >> gcs_to_bq >> delete_bucket
