# from datetime import datetime, timedelta

# # from airflow import DAG

# from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
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

# store these as env variables in docker?
logs_folder = '/home/oskar/logs'
logs_json_folder = os.path.join(logs_folder, 'logs_json')

with DAG(
    dag_id='network_packet_logger',
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=1),
    schedule='@hourly',
    catchup=False,
) as dag:
    
    # TOOD should terraform init be ran inside a airflow dag?
    # there must be a proper way to do this
    # or terraflow should init airflow?
    terraform_start = BashOperator(
        task_id='terraform_start',
        bash_command='cd /home/oskar/airflow-demo && terraform init && terraform plan --out 1.plan && terraform apply "1.plan"',
    )

    # could be done using bashoperator for sure
    @task()
    def get_raw_log_files():
        # TODO make sure file is finished. maybe skip the last file?
        files = os.listdir(logs_folder)
        files = [os.path.join(logs_folder, f) for f in files]
        # keep only files. not folders
        files = [f for f in files if os.path.isfile(f)]
        return files
    
    # should a output folder exist? or just a permanent temp folder somewhere?
    @task()
    def make_output_folder():
        os.makedirs(logs_json_folder, exist_ok=True)

    @task()
    def remove_file(filepath):
        # had to add -f flag. scheduler was stopping due to asking if to remove wirte protected file

        print(f"Removing {filepath}")

        # commented out to keep the task in graph but not remove files for development purposes
        # os.system(f'rm -f {filepath}')

    # https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
    @task()
    def process_raw_file(input_file):

        # file processing makes running the dag concurrently throw errors
        # probably multiple access to the same file by multiple processes

        # TODO convert to bash operator
        output_file = os.path.join(logs_json_folder, os.path.basename(input_file) + '.json')
        print("creating", output_file, "file from", input_file)
        os.system(f"tshark -r {input_file} -T json > {output_file}")

        # Read the JSON file
        with open(output_file, 'r') as f:
            logs = json.load(f)

        def parse_log(d):
            res = {}

            layers = d.get('_source', {}).get('layers', {})
            frame = layers.get('frame', {})
            ip = layers.get('ip', {})
            tcp = layers.get('tcp', {})
            data = layers.get('data', {})

            ts = frame.get('frame.time')
            ts = ts.rsplit(' ', 1)[0]  # Remove the time zone
            ts = ts[:ts.rfind('.') + 7] # only keep 6 fractional digits

            # for some reason cant json.dumps datetime object
            # ts = datetime.strptime(ts, '%b %d, %Y %H:%M:%S.%f')  # Parse the date and time
            
            res['timestamp'] = ts
            # print(ts)

            res['ip_src'] = ip.get('ip.src')
            res['ip_dst'] = ip.get('ip.dst')

            res['tcp_srcport'] = tcp.get('tcp.srcport')
            res['tcp_dstport'] = tcp.get('tcp.dstport')

            res['data_len'] = data.get('data.len')

            print(json.dumps(res))

            return res
        
        logs = [parse_log(log) for log in logs]

        # Convert JSON objects to newline delimited JSON strings
        jsonl_strings = [json.dumps(log) for log in logs]

        # Write each string to a new line in the output file
        with open(output_file, 'w') as f:
            f.write('\n'.join(jsonl_strings))

        return output_file

    @task
    def reduce(values):
        return list(values)

    make_output_folder_task = make_output_folder()

    raw_log_files = get_raw_log_files()

    # TODO does this need to be reduced before uploading files from output folder?
    files_json = process_raw_file.expand(input_file=raw_log_files)
    
    files_json_reduced = reduce(files_json)

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src=files_json_reduced,
        dst='', # '/' creates a folder in bucket
        bucket='logs-123214455'
    )
    
    gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='logs-123214455',
        source_objects='logs.json',
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition="CREATE_IF_NEEDED",
        # schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
        destination_project_dataset_table="smiling-rhythm-400807:test.logs"
    )

    remove_raw_files_task = remove_file.expand(filepath=raw_log_files)

    [make_output_folder_task] >> files_json >> [remove_raw_files_task, upload_to_gcs]
    [make_output_folder_task] >> files_json

    remove_json_files_task = remove_file.expand(filepath=files_json)
    terraform_start >> upload_to_gcs >> [remove_json_files_task, gcs_to_bq]
