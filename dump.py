from pymysql.converters import escape_string
from sqlalchemy import create_engine
import datetime

# Database Connection Details
username = 'memberyo'
password = '123456'
host = 'localhost'
database = 'memberyo'

# Establish a Database Connection
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{database}')
conn = engine.connect()

# List of table SQL file mappings
tables_to_files = {
    'identity_verification': 'dump_identity_verification_table.sql',
    # Add the other table to filename mappings here as needed...
}

# Function to read the SQL command from a file
def read_sql_command(file_path):
    with open(file_path, 'r') as file:
        return file.read().strip()

# Function to escape values based on type
def escape_value(item):
    if isinstance(item, str):
        return f"'{escape_string(item)}'"
    elif isinstance(item, datetime.datetime):
        return f"'{item.strftime('%Y-%m-%d %H:%M:%S')}'"
    elif isinstance(item, datetime.date):
        return f"'{item.strftime('%Y-%m-%d')}'"
    else:
        return str(item)

# Function to dump a table to an SQL file using a command from an SQL file
def dump_table_to_sql(table_name, sql_command):
    with conn.connection.cursor() as cursor:
        cursor.execute(sql_command)
        rows = cursor.fetchall()

        with open(f"data/dump_{table_name}.sql", 'w') as file:
            for row in rows:
                values = ', '.join(escape_value(item) for item in row)
                insert_statement = f"INSERT INTO {table_name} VALUES ({values});\n"
                file.write(insert_statement)

# Execute the command from each SQL file
for table_name, file_name in tables_to_files.items():
    sql_command = read_sql_command(file_name)
    print(f"Dumping table: {table_name}")
    dump_table_to_sql(table_name, sql_command)

# Close the connection
conn.close()
