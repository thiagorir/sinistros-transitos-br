import os
import csv
from dotenv import load_dotenv
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound

# Load env variables
load_dotenv()

# GCP Configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
GCP_SERVICE_ACCOUNT_KEY_PATH = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH")
BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")

# Basic check to ensure credentials are loaded
if not all([GCP_PROJECT_ID, GCP_BUCKET_NAME, GCP_SERVICE_ACCOUNT_KEY_PATH, BIGQUERY_DATASET_ID]):
    raise ValueError("One or more GCP environment variables not found. Make sure your .env file is correctly configured.")

# Initialize GCP clients
try:
    storage_client = storage.Client.from_service_account_json(GCP_SERVICE_ACCOUNT_KEY_PATH, project=GCP_PROJECT_ID)
    bigquery_client = bigquery.Client.from_service_account_json(GCP_SERVICE_ACCOUNT_KEY_PATH, project=GCP_PROJECT_ID)
    print(f"Successfully connected to GCP Project: {GCP_PROJECT_ID}")
except Exception as e:
    raise RuntimeError(f"Failed to initialize GCP clients. Check your service account key path and permissions: {e}")

# Helper function for data type inference
def infer_column_type(value):
    
    # Infers the BigQuery data type from a given value.
    
    if value is None or value == '':
        return 'STRING'
    if isinstance(value, bool):
        return 'BOOLEAN'
    try:
        if float(value) == int(float(value)):
            return 'INTEGER'
    except ValueError:
        pass
    try:
        float(value)
        return 'FLOAT'
    except ValueError:
        pass
    return 'STRING'

def get_table_schema_from_csv(file_path: str, num_rows_for_inference: int = 100) -> list[bigquery.SchemaField] | None:
    
    # Infers table schema (column names and BigQuery types) from a CSV file.
    
    columns_info = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            headers = next(reader)

            for header in headers:
                sanitized_header = ''.join(c if c.isalnum() or c == '_' else '_' for c in header).lower()
                columns_info[sanitized_header] = {'name': sanitized_header, 'type': 'STRING'}

            rows_for_inference = []
            for i, row in enumerate(reader):
                if i < num_rows_for_inference:
                    rows_for_inference.append(row)
                else:
                    break

            for row_idx, row in enumerate(rows_for_inference):
                for j, value in enumerate(row):
                    if j < len(headers):
                        header_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in headers[j]).lower()
                        current_type = columns_info[header_name]['type']
                        inferred_type = infer_column_type(value)

                        if inferred_type == 'STRING' and current_type != 'STRING':
                            columns_info[header_name]['type'] = 'STRING'
                        elif inferred_type == 'FLOAT' and current_type == 'INTEGER':
                            columns_info[header_name]['type'] = 'FLOAT'

        bq_schema_fields = []
        for col_name, info in columns_info.items():
            bq_schema_fields.append(bigquery.SchemaField(info['name'], info['type'], mode='NULLABLE'))

        return bq_schema_fields
    except Exception as e:
        print(f"Error inferring schema from '{file_path}': {e}")
        return None

def ensure_bigquery_dataset_exists(dataset_id: str):
    
    # Ensures that the BigQuery dataset exists, creating it if necessary.
    
    dataset_ref = bigquery_client.dataset(dataset_id)
    try:
        bigquery_client.get_dataset(dataset_ref)
        print(f"BigQuery Dataset '{dataset_id}' already exists.")
    except NotFound:
        print(f"BigQuery Dataset '{dataset_id}' not found. Creating it...")
        bigquery_client.create_dataset(dataset_ref)
        print(f"BigQuery Dataset '{dataset_id}' created.")

def ensure_bigquery_table_exists(table_id: str, bq_schema: list[bigquery.SchemaField]):
    
    # Ensures that the BigQuery table exists with the specified schema and creates it if it doesn't exist.
    
    dataset_ref = bigquery_client.dataset(BIGQUERY_DATASET_ID)
    table_ref = dataset_ref.table(table_id)

    try:
        bigquery_client.get_table(table_ref)
        print(f"BigQuery Table '{BIGQUERY_DATASET_ID}.{table_id}' already exists.")
        return True
    except NotFound:
        print(f"BigQuery Table '{BIGQUERY_DATASET_ID}.{table_id}' not found. Creating it...")
        table = bigquery.Table(table_ref, schema=bq_schema)
        bigquery_client.create_table(table)
        print(f"BigQuery Table '{BIGQUERY_DATASET_ID}.{table_id}' created successfully.")
        return True
    except Exception as e:
        print(f"Error checking/creating BigQuery table '{BIGQUERY_DATASET_ID}.{table_id}': {e}")
        return False

def upload_csv_to_gcs(file_path: str, filename: str, gcs_bucket_name: str, gcs_prefix: str = "raw_csv_uploads/"):
    
    # Uploads a CSV file to Google Cloud Storage.
    
    bucket = storage_client.bucket(gcs_bucket_name)
    blob_name = f"{gcs_prefix}{filename}"
    blob = bucket.blob(blob_name)

    print(f"Uploading '{filename}' to GCS bucket '{gcs_bucket_name}' as '{blob_name}'...")
    blob.upload_from_filename(file_path)
    gcs_uri = f"gs://{gcs_bucket_name}/{blob_name}"
    print(f"Uploaded '{filename}' to GCS: {gcs_uri}")
    return gcs_uri

def load_gcs_csv_to_bigquery(gcs_uri: str, table_id: str, bq_schema: list[bigquery.SchemaField], filename: str):
    
    # Loads data from a GCS CSV file into a BigQuery table.
    
    dataset_ref = bigquery_client.dataset(BIGQUERY_DATASET_ID)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=bq_schema,
        field_delimiter=";",
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    print(f"Loading data from '{gcs_uri}' into BigQuery table '{BIGQUERY_DATASET_ID}.{table_id}'...")
    load_job = bigquery_client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )

    try:
        load_job.result()
        print(f"Successfully loaded data from '{filename}' into BigQuery table '{BIGQUERY_DATASET_ID}.{table_id}'.")
        return True
    except Exception as e:
        print(f"Failed to load data from '{filename}' into BigQuery table '{BIGQUERY_DATASET_ID}.{table_id}': {load_job.errors or e}")
        return False

def process_and_upload_csv_data(file_path: str):
    
    # Parses the filename, uploads CSV to GCS, and loads data into BigQuery.
    
    filename = os.path.basename(file_path)
    base_name, file_extension = os.path.splitext(filename)

    if file_extension.lower() != ".csv":
        print(f"Skipping '{filename}': Not a CSV file.")
        return
    if '_DadosAbertos_' not in base_name:
        print(f"Skipping '{filename}': CSV file does not contain '_DadosAbertos_' in its name, which is required for table/partition determination.")
        return

    try:
        parts = base_name.split('_DadosAbertos_')
        table_id = parts[0].lower()
        partition_value = parts[1]

        if not table_id:
            print(f"Could not determine table ID for '{filename}'. Skipping.")
            return
        if not partition_value:
            print(f"Could not determine partition value for '{filename}'. Skipping.")
            return

        print(f"Determined BigQuery Table ID: '{table_id}', Partition Value: '{partition_value}'")

        ensure_bigquery_dataset_exists(BIGQUERY_DATASET_ID)

        bq_schema = get_table_schema_from_csv(file_path)
        if not bq_schema:
            print(f"Failed to infer BigQuery schema for '{filename}'. Cannot proceed.")
            return

        if not ensure_bigquery_table_exists(table_id, bq_schema):
            print(f"Could not ensure BigQuery table '{BIGQUERY_DATASET_ID}.{table_id}' exists. Skipping data load for '{filename}'.")
            return

        gcs_uri = upload_csv_to_gcs(file_path, filename, GCP_BUCKET_NAME)
        if not gcs_uri:
            print(f"Failed to upload '{filename}' to GCS. Skipping BigQuery load.")
            return

        if not load_gcs_csv_to_bigquery(gcs_uri, table_id, bq_schema, filename):
             print(f"Failed to load data from '{filename}' into BigQuery. Check BigQuery job logs for details.")

    except Exception as e:
        print(f"An unexpected error occurred while processing '{filename}': {e}")


def process_directory(directory_path: str):
    
    # Reads a directory and attempts to process and upload data from each CSV file to GCP (GCS then BigQuery) based on filename patterns.
    
    if not os.path.isdir(directory_path):
        print(f"Error: The provided path '{directory_path}' is not a valid directory.")
        return

    print(f"Processing directory: {directory_path}")
    processed_files_count = 0

    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)

        if os.path.isfile(file_path):
            print(f"\n--- Processing file: {filename} ---")
            process_and_upload_csv_data(file_path)
            processed_files_count += 1

    print("\n--- Processing Summary ---")
    print(f"Total files attempted to process: {processed_files_count}")
    print("Please check the logs above for specific success/failure messages for each file.")


if __name__ == "__main__":
    print("--- Starting GCP CSV Uploader ---")
    directory_to_upload = input("Please enter the full path to the directory containing your CSV files: ")
    process_directory(directory_to_upload)
    print("\nScript finished.")
