import json
import pandas as pd
import requests


def get_db_details(dbname):
    request_url = 'http://data-api-alb-2-946058755.us-west-2.elb.amazonaws.com/api/v1/resources/catalog/database?dbname=' + dbname
    databaseName = requests.get(request_url)
    dbname = databaseName.text
    return dbname


def get_glue_table(dbname, tablename):
    request_url = 'http://data-api-alb-2-946058755.us-west-2.elb.amazonaws.com/api/v1/resources/catalog/table?dbname=' + dbname + '&tablename=' + tablename
    dbTableName = requests.get(request_url)
    TableName = dbTableName.text
    df = pd.DataFrame(json.loads(TableName)['data'])
    return df


def get_s3_part_parquet(bucket, key):
    request_url = 'http://data-api-alb-2-946058755.us-west-2.elb.amazonaws.com/api/v1/resources/s3/partitioned_file' + '?bucket=' + bucket + '&key=' + key
    s3_object = requests.get(request_url)
    object_df = s3_object.text
    df = pd.DataFrame(json.loads(object_df)['data'])
    return df


def get_rs_data():
    request_url = "http://data-api-alb-2-946058755.us-west-2.elb.amazonaws.com/api/v1/resources/redshift/get_table_data"
    call_rs_data = requests.get(request_url)
    rs_text_dump = call_rs_data.text
    rs_output_dict = json.loads(rs_text_dump)
    rs_data = rs_output_dict['data']
    rs_column = rs_output_dict['redshift_column_name']
    df = pd.DataFrame(rs_data, columns=rs_column)
    return df


if __name__ == "__main__":
    """
    Input should always be glue database and table name.
    Based the database and table names, find if the able is in S3 or RS.
    If S3 - see if the files are partitioned or not, and the use the appropriate method.
    If RS - use psycopg2 and extract the data. Store secrets in AWS Secrets Manager.
    """
    # customer_data = get_s3_part_parquet('kpk-dl-raw', '/customer/') # add file name part.
    # print(customer_data)
    # glue_tb_db = get_glue_table('spectrumdb', 'customer') # spectrum table.
    # print(glue_tb_db)
    print(get_rs_data()) # Redshift table.