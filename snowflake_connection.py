from pydoc import importfile
import snowflake.connector
from .sql_ddl import create_refresh_table
import json
import boto3
secrets_manager = boto3.client('secretsmanager')


def connect_to_snowflake(environment, set_cursor = True): # connect_to_snowflake(snowflake_role, environment, set_cursor = True)
        snowflake_secrets_by_env = {"dev" : "kwa-snowflake-dev"}
        secret_name = snowflake_secrets_by_env[environment]
#        role = "role_{}".format(snowflake_role)
        credential_info = secrets_manager.get_secret_value(SecretId = secret_name)['SecretString']
        ACCOUNT = json.loads(credential_info)['account']
        USERNAME = json.loads(credential_info)['user']
        PASSWORD = json.loads(credential_info)['password']
        DATABASE = json.loads(credential_info)['database']
        WAREHOUSE = json.loads(credential_info)['warehouse']
        ROLE = json.loads(credential_info)['role']
        SCHEMA =json.loads(credential_info)['schema']
        try:
            conn = snowflake.connector.connect(
                account = ACCOUNT,
                user = USERNAME,
                password = PASSWORD,
                role = ROLE,
                warehouse = WAREHOUSE,
                database = DATABASE,
                schema = SCHEMA
            )
        except Exception as e:
            print("Unable to connect to Snowflake: " + repr(e))
        else:
            if set_cursor is True:
                connection = conn.cursor()
            else:
                connection = conn
            return connection
    

def snowflake_cursor():
    # create cursor object
    conn = connect_to_snowflake('dev',set_cursor=True)
    conn.execute(create_refresh_table)
    print("Table is refreshed")
