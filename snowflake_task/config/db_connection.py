from snowflake.snowpark.session import Session
import os
from dotenv import load_dotenv

load_dotenv()

connection_params = {
    "account": os.getenv("account"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "role": os.getenv("role"),
    "warehouse": os.getenv("warehouse")
}

session = Session.builder.configs(connection_params).create()