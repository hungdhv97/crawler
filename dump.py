import datetime
import hashlib
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

# Dictionary of tables and their primary key columns
table_primary_keys = {
    'address': 'address_id',
    'admin_audit_log': 'admin_audit_log',
    'agreement': 'agreement_id',
    'block': 'bock_id',
    'customer': 'customer_id',
    'dormant_customer': 'dormant_customer_id',
    'dowant_user': 'dowant_user_id',
    'identity_verification': 'identity_verification_id',
    'phone_certification': 'phone_certification_id',
    'privacy_removal': 'privacy_removal_id',
    'reserved_withdrawal': 'reserved_withdrawal_id',
    'social': 'social_id',
}


# Function to generate random birthday
def random_birthday():
    start_date = datetime.date(1970, 1, 1)
    end_date = datetime.date(2000, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + datetime.timedelta(days=random_days)


# Function to hash a value with SHA-512 and return the first n characters
def create_hashed_subset(value, length):
    if value is not None:
        hashed_value = hashlib.sha512(str(value).encode()).hexdigest()
        return hashed_value[:length]
    else:
        return None


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


# Function to get total row count of a table
def get_total_row_count(table_name):
    try:
        sql_command = f"SELECT COUNT(*) FROM {table_name};"
        with conn.connection.cursor() as cursor:
            cursor.execute(sql_command)
            result = cursor.fetchone()
            return result[0]  # Return the count
    except Exception as e:
        print(f"Error fetching row count for table {table_name}: {e}")
        return 0


# Function to dump a table to an SQL file with pagination
def dump_table_to_sql(table_name, primary_key_column, columns, batch_size=100):
    try:
        last_id = 0
        total_rows_dumped = 0
        file_path = f"dump_{table_name}_table.sql"
        total_rows_table = get_total_row_count(table_name)
        with open(file_path, 'w') as file:
            while True:
                sql_command = f"SELECT * FROM {table_name} WHERE {primary_key_column} > {last_id} ORDER BY id ASC LIMIT {batch_size};"
                with conn.connection.cursor() as cursor:
                    cursor.execute(sql_command)
                    rows = cursor.fetchall()
                    rows_fetched = len(rows)
                    if not rows:
                        break  # Break the loop if no more rows are fetched

                    for row in rows:
                        last_id = row[0]
                        row = list(row)
                        # Update random fields and generate insert statement as before
                        if 'birthday' in columns:
                            row[columns.index('birthday')] = random_birthday()
                        if 'ci' in columns:
                            row[columns.index('ci')] = create_hashed_subset(row[columns.index('ci')], 88)
                        if 'di' in columns:
                            row[columns.index('di')] = create_hashed_subset(row[columns.index('di')], 64)
                        if 'phone' in columns:
                            row[columns.index('phone')] = random_phone()
                        if 'email' in columns:
                            row[columns.index('email')] = random_email()

                        # Generate insert statement
                        values = ', '.join(escape_value(item) for item in row)
                        insert_statement = f"INSERT INTO {table_name} VALUES ({values});\n"
                        file.write(insert_statement)

                    total_rows_dumped += rows_fetched
                    print(f"Dumped {total_rows_dumped}/{total_rows_table} rows from {table_name}")

        print(f"Successfully dumped table: {table_name}")
    except Exception as e:
        print(f"Error dumping table {table_name}: {e}")


# Dump each table
for table, primary_key in table_primary_keys.items():
    columns = get_columns_for_table(table)
    if columns:
        dump_table_to_sql(table, primary_key, columns)
    else:
        print(f"Skipping table {table} due to column fetch error.")
    print("---------------------")

# Close the connection
conn.close()
