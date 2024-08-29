import functions_framework
import yaml
import snowflake.connector
from google.cloud import storage
import os

@functions_framework.cloud_event
def load_data_to_snowflake(cloud_event):
    # 连接到 Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    cursor = conn.cursor()

    # 使用环境变量中定义的 schema
    cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_SCHEMA')};")

    # 从 Cloud Event 中提取 GCS bucket 和文件名
    bucket_name = cloud_event.data["bucket"]
    file_name = cloud_event.data["name"]

    # 从 GCS 下载文件
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_content = blob.download_as_string().decode('utf-8')

    # 调试输出，检查文件内容
    print("File content:", file_content)

    # 加载 YAML 配置文件
    yaml_data = yaml.safe_load(file_content)

    # 调试输出，检查解析后的 YAML 数据
    print("YAML data:", yaml_data)

    # 获取表名
    table_name = yaml_data.get("raw_table_name", "CUSTOMER_TABLE")

    # 如果表不存在，则创建表
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

    # 构建 COPY INTO 查询语句
    copy_into_query = f"""
    COPY INTO {table_name}
    FROM @my_stage/{file_name}
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = CONTINUE;
    """
    
    # 执行查询，将数据加载到 Snowflake 中
    cursor.execute(copy_into_query)
    print(f"Data loaded into {table_name} successfully.")

    conn.commit()
    cursor.close()
    conn.close()
