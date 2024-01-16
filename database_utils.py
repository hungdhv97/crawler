import datetime

from pymysql.converters import escape_string
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from hashing_utils import hash_value
from random_utils import generate_random_birthday, generate_random_phone, generate_random_email


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


class MySQLDatabase:
    def __init__(self, db_username, db_password, db_host, db_port, db_name):
        self.engine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
        self.conn = None

    def create_db_connection(self):
        self.conn = self.engine.connect()
        return self.conn

    def close_db_connection(self):
        if self.conn:
            self.conn.close()

    # Function to get column names for a table
    def get_columns_for_table(self, table_name):
        try:
            sql_command = f"SHOW COLUMNS FROM {table_name};"
            with self.conn.connection.cursor() as cursor:
                cursor.execute(sql_command)
                return [row[0] for row in cursor.fetchall()]
        except SQLAlchemyError as e:
            print(f"SQLAlchemyError occurred in table '{table_name}': {e}")
            return []
        except Exception as e:
            print(f"Error occurred in table '{table_name}': {e}")
            return []

    # Function to get total row count of a table
    def get_total_row_count(self, table_name):
        try:
            sql_command = f"SELECT COUNT(*) FROM {table_name};"
            with self.conn.connection.cursor() as cursor:
                cursor.execute(sql_command)
                result = cursor.fetchone()
                return result[0]
        except SQLAlchemyError as e:
            print(f"SQLAlchemyError occurred in table '{table_name}': {e}")
            return []
        except Exception as e:
            print(f"Error occurred in table '{table_name}': {e}")
            return []

    def dump_table_to_sql(self, table_name, primary_key_column, batch_size):
        columns = self.get_columns_for_table(table_name)
        if columns:
            try:
                last_id = 0
                total_rows_dumped = 0
                file_path = f"dump_{table_name}_table.sql"
                total_rows_table = self.get_total_row_count(table_name)
                with open(file_path, 'w') as file:
                    while True:
                        sql_command = f"SELECT * FROM {table_name} WHERE {primary_key_column} > {last_id} ORDER BY {primary_key_column} ASC LIMIT {batch_size};"
                        with self.conn.connection.cursor() as cursor:
                            cursor.execute(sql_command)
                            rows = cursor.fetchall()
                            rows_fetched = len(rows)
                            if not rows:
                                break

                            for row in rows:
                                last_id = row[0]
                                row = list(row)
                                if 'birthday' in columns:
                                    row[columns.index('birthday')] = generate_random_birthday()
                                if 'ci' in columns:
                                    row[columns.index('ci')] = hash_value(row[columns.index('ci')], 88)
                                if 'di' in columns:
                                    row[columns.index('di')] = hash_value(row[columns.index('di')], 64)
                                if 'phone' in columns:
                                    row[columns.index('phone')] = generate_random_phone()
                                if 'email' in columns:
                                    row[columns.index('email')] = generate_random_email()

                                # Generate insert statement
                                values = ', '.join(escape_value(item) for item in row)
                                insert_statement = f"INSERT INTO {table_name} VALUES ({values});\n"
                                file.write(insert_statement)

                            total_rows_dumped += rows_fetched
                            print(f"Dumped {total_rows_dumped}/{total_rows_table} rows from {table_name}")

                print(f"Successfully dumped table: {table_name}")
            except Exception as e:
                print(f"Error dumping table {table_name}: {e}")
        else:
            print(f"Skipping table {table_name} due to column fetch error.")
        print("---------------------")
