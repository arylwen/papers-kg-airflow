from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def list_files_from_s3(bucket, prefix_value):
    s3_hook = S3Hook(aws_conn_id="minio_airflow")
    paths = s3_hook.list_keys(bucket_name=bucket, prefix=prefix_value)
    return paths