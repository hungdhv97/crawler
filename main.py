import os

from dotenv import load_dotenv

from database_utils import MySQLDatabase

# Load environment variables from .env file
load_dotenv()

# Environment variables for database configuration
db_host = os.getenv("DB_RO_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("MEMBERYO_DB_NAME")
db_username = os.getenv("MEMBERYO_DB_RO_USERNAME")
db_password = os.getenv("MEMBERYO_DB_RO_PASSWORD")

# Dictionary of tables and their primary key columns
table_primary_keys = {
    'address': 'address_id',
    'admin_audit_log': 'admin_audit_log_id',
    'agreement': 'agreement_id',
    'block': 'block_id',
    'customer': 'customer_id',
    'dormant_customer': 'customer_id',
    'dowant_user': 'dowant_user_id',
    'identity_verification': 'identity_verification_id',
    'phone_certification': 'phone_certification_id',
    'privacy_removal': 'privacy_removal_id',
    'reserved_withdrawal': 'customer_id',
    'social': 'social_id',
}

# Main script execution
if __name__ == '__main__':
    # Initialize MySQL object with database credentials
    mysql = MySQLDatabase(db_username, db_password, db_host, db_port, db_name)

    # Create a database connection
    mysql.create_db_connection()

    # Dump each table
    for table_name, primary_key_column in table_primary_keys.items():
        mysql.dump_table_to_sql(table_name, primary_key_column, batch_size=100)

    # Ensure the database connection is closed after operations
    mysql.close_db_connection()
