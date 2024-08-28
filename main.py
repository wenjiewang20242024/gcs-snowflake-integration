import functions_framework
import yaml
import snowflake.connector
from google.cloud import storage
from flask import request, jsonify
import os

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

        # Fetch pending load tasks from the loading table
        cursor.execute("SELECT file_name, target_table FROM loading_table WHERE status='pending'")
        loading_tasks = cursor.fetchall()

        for task in loading_tasks:
            file_name, target_table = task

            # Load data from the file in GCS to Snowflake
            load_query = f"""
            COPY INTO {target_table}
            FROM '@my_stage/{file_name}'
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            """
            cursor.execute(load_query)
            
            # Update the task status to 'completed'
            cursor.execute(f"UPDATE loading_table SET status='completed' WHERE file_name='{file_name}'")
        
        conn.commit()
        cursor.close()
        conn.close()

        return "Data loading completed successfully", 200

    except Exception as e:
        print(f"Unhandled Exception: {str(e)}")
        return f"Error: {str(e)}", 500
