import functions_framework
import yaml
import snowflake.connector
from google.cloud import storage
import os

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

    # Load YAML configuration file
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

    # Construct the COPY INTO query
    copy_into_query = f"""
    COPY INTO {table_name}
    FROM @my_stage/{file_name}
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = CONTINUE;
    """
    
    # Execute the query to load data into Snowflake
    cursor.execute(copy_into_query)
    print(f"Data loaded into {table_name} successfully.")

    conn.commit()
    cursor.close()
    conn.close()
