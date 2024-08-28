import functions_framework
import yaml
import snowflake.connector
from google.cloud import storage
import os
import csv
import io

@functions_framework.cloud_event
def load_data_to_snowflake(cloud_event):
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    cursor = conn.cursor()

    # Use the schema defined in the environment variable
    cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_SCHEMA')};")

    # Extract the GCS bucket and file name from the Cloud Event
    bucket_name = cloud_event.data["bucket"]
    file_name = cloud_event.data["name"]

    # Download the file from GCS
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_content = blob.download_as_string().decode('utf-8')

    print(f"File content: {file_content}")

    # Check if the file is YAML or CSV
    if file_name.endswith('.yaml') or file_name.endswith('.yml'):
        # Handle YAML file
        try:
            yaml_data = yaml.safe_load(file_content)
            print(f"YAML data: {yaml_data}")
        except yaml.YAMLError as exc:
            print(f"Error parsing YAML: {exc}")
            raise

        if not isinstance(yaml_data, dict):
            print(f"YAML data is not a dictionary. Type: {type(yaml_data)}")
            raise ValueError("YAML data should be a dictionary")

        table_name = yaml_data.get("raw_table_name", "CUSTOMER_TABLE")

        # Create the table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ID INTEGER,
            NAME STRING,
            AGE INTEGER,
            EMAIL STRING,
            JOIN_DATE DATE
        );
        """
        cursor.execute(create_table_query)
        print(f"Table {table_name} created or already exists.")

    elif file_name.endswith('.csv'):
        # Handle CSV file
        table_name = "CUSTOMER_TABLE"  # Or extract this from somewhere else
        csv_reader = csv.reader(io.StringIO(file_content))
        headers = next(csv_reader)  # Skip the header row
        for row in csv_reader:
            # Insert each row into the table
            insert_query = f"""
            INSERT INTO {table_name} ({', '.join(headers)})
            VALUES ({', '.join([f"'{value}'" for value in row])});
            """
            cursor.execute(insert_query)
        print(f"Data loaded into {table_name} successfully.")
    else:
        raise ValueError("Unsupported file format")

    conn.commit()
    cursor.close()
    conn.close()
