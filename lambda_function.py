from src.snowflake_s3_connection import connect_to_snowflake,snowflake_cursor
import json

def lambda_handler(event, context): 
    print('hi')
    print('hello')
    snowflake = snowflake_cursor()
