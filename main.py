import functions_framework
import snowflake.connector
from google.cloud import storage
import os

# load_data_to_snowflake function
@functions_framework.http
def load_data_to_snowflake(request):
    try:
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

        # Retrieve pending tasks from the loading_table
        cursor.execute("SELECT file_name, target_table FROM loading_table WHERE status='pending'")
        loading_tasks = cursor.fetchall()

        storage_client = storage.Client()

        for task in loading_tasks:
            file_name, target_table = task

            # Specify the GCS bucket name
            bucket_name = 'my_data_bucket_upload'  # Your current GCS bucket name
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(file_name)
            local_path = f'/tmp/{file_name}'

            # Download the file from GCS to a local path
            blob.download_to_filename(local_path)

            # Load data from the local file into Snowflake
            load_query = f"""
            COPY INTO {target_table}
            FROM @~/staged/{file_name}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            """

            cursor.execute(load_query)

            # Update task status to 'completed'
            cursor.execute(f"UPDATE loading_table SET status='completed' WHERE file_name='{file_name}'")

        conn.commit()
        cursor.close()
        conn.close()

        return "Data loading completed successfully", 200

    except Exception as e:
        print(f"Unhandled Exception: {str(e)}")
        return f"Error: {str(e)}", 500
