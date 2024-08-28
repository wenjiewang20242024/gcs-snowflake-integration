import functions_framework
import yaml
import snowflake.connector
from google.cloud import storage
import os

# Define the function to be triggered by Cloud Storage
@functions_framework.cloud_event
def load_data_to_snowflake(cloud_event):
    try:
        # Extract the bucket name and file name from the event
        bucket_name = cloud_event.data['bucket']
        file_name = cloud_event.data['name']

        # Initialize GCS client and download the file
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        yaml_content = blob.download_as_string()

        # Parse the YAML file
        yaml_data = yaml.safe_load(yaml_content)

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

        # Load data into Snowflake
        for key, value in yaml_data.items():
            cursor.execute(f"""
                MERGE INTO {value['raw_table_name']} t
                USING (SELECT '{key}' AS key, '{value}' AS value) s
                ON t.key = s.key
                WHEN MATCHED THEN
                    UPDATE SET t.value = s.value
                WHEN NOT MATCHED THEN
                    INSERT (key, value) VALUES (s.key, s.value)
            """)

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Successfully loaded data from {file_name} into Snowflake")

    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise e
