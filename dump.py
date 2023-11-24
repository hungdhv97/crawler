import datetime
import os
import random
import string

from dotenv import load_dotenv
from pymysql.converters import escape_string
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()

# Extract database connection details from environment variables
db_host = os.getenv("DB_RO_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("MEMBERYO_DB_NAME")
db_username = os.getenv("MEMBERYO_DB_RO_USERNAME")
db_password = os.getenv("MEMBERYO_DB_RO_PASSWORD")

# Establish a Database Connection
engine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
conn = engine.connect()

# List of tables to dump
tables = [
    'address',
    'admin_audit_log',
    'agreement',
    'block',
    'customer',
    'dormant_customer',
    'dowant_user',
    'identity_verification',
    'phone_certification',
    'privacy_removal',
    'reserved_withdrawal',
    'social',
]


# Function to generate random birthday
def random_birthday():
    start_date = datetime.date(1970, 1, 1)
    end_date = datetime.date(2000, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + datetime.timedelta(days=random_days)


# Function to generate a random number with specified digits
def random_number(digits):
    range_start = 10 ** (digits - 1)
    range_end = (10 ** digits) - 1
    return str(random.randint(range_start, range_end))


# Function to generate random phone number
def random_phone():
    return "010" + str(random.randint(10000000, 99999999))


# Function to generate random email
def random_email():
    username_length = random.randint(5, 10)
    username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=username_length))

    domain = "@example.com"
    return username + domain


# Function to escape values based on type
def escape_value(item):
    if item is None:
        return 'NULL'
    if isinstance(item, str):
        return f"'{escape_string(item)}'"
    elif isinstance(item, datetime.datetime):
        return f"'{item.strftime('%Y-%m-%d %H:%M:%S')}'"
    elif isinstance(item, datetime.date):
        return f"'{item.strftime('%Y-%m-%d')}'"
    else:
        return str(item)


# Function to get column names for a table
def get_columns_for_table(table_name):
    try:
        sql_command = f"SHOW COLUMNS FROM {table_name};"
        with conn.connection.cursor() as cursor:
            cursor.execute(sql_command)
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error fetching columns for table {table_name}: {e}")
        return []  # Return an empty list if there's an error


# Function to dump a table to an SQL file
def dump_table_to_sql(table_name, columns):
    try:
        sql_command = f"SELECT * FROM {table_name};"
        with conn.connection.cursor() as cursor:
            cursor.execute(sql_command)
            rows = cursor.fetchall()

            with open(f"data/dump_{table_name}_table.sql", 'w') as file:
                for row in rows:
                    row = list(row)
                    # Update random fields if present in the table
                    if 'birthday' in columns:
                        row[columns.index('birthday')] = random_birthday()
                    if 'ci' in columns:
                        row[columns.index('ci')] = random_number(88)
                    if 'di' in columns:
                        row[columns.index('di')] = random_number(64)
                    if 'phone' in columns:
                        row[columns.index('phone')] = random_phone()
                    if 'email' in columns:
                        row[columns.index('email')] = random_email()

                    # Generate insert statement
                    values = ', '.join(escape_value(item) for item in row)
                    insert_statement = f"INSERT INTO {table_name} VALUES ({values});\n"
                    file.write(insert_statement)
        print(f"Successfully dumped table: {table_name}")
    except Exception as e:
        print(f"Error dumping table {table_name}: {e}")


# Dump each table
for table in tables:
    columns = get_columns_for_table(table)
    if columns:
        dump_table_to_sql(table, columns)
    else:
        print(f"Skipping table {table} due to column fetch error.")
    print("---------------------")

# Close the connection
conn.close()
