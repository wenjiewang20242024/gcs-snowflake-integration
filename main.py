import functions_framework
import yaml
import snowflake.connector
from google.cloud import storage
import os

# This function triggers when a file is uploaded to the specified Cloud Storage bucket
@functions_framework.cloud_event
def load_data_to_snowflake(event):
    try:
        # Get the bucket and file name from the event
        bucket_name = event.data["bucket"]
        file_name = event.data["name"]
        
        # Initialize GCS client and download the file
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        yaml_content = blob.download_as_string()

        # Parse the YAML file (assuming the uploaded file is YAML)
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

        # Automatically detect changes and update the table
        for key, value in yaml_data.items():
            # Upsert data into Snowflake table
            cursor.execute(f"""
                MERGE INTO your_table_name t
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

        print(f"Processed file {file_name} from bucket {bucket_name} successfully.")
    
    except Exception as e:
        print(f"Error processing file {file_name}: {str(e)}")
