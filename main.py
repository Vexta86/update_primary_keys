import mysql.connector
from utils import json_to_dict
import os

# Access environment variables
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")

# Connect to MySQL
db_connection = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)

cursor = db_connection.cursor()

DATE = '0123'

schema = json_to_dict('schema_carlos_vieco.json')

# Step 1: Get a list of tables
cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db_name}';")
tables = cursor.fetchall()

failed = []
can_truncate = ('Evaluacion_director_grupo', 'Promedio_puesto', 'Registro_comportamental1', 'Registro_comportamental4',
                'Alerta_academica1', 'Alerta_academica2', 'Alerta_academica3', 'Alerta_academica4',
                'Mensajes_adm',)
can_skip = ('Promedio_puesto')


def backup_table(original_name):
    # Backup the original table
    create_table_like = f"CREATE TABLE {original_name}_{DATE} LIKE {original_name}"
    insert_in_backup = f"INSERT INTO {original_name}_{DATE} SELECT * FROM {original_name}"

    cursor.execute(create_table_like)
    print(create_table_like)
    db_connection.commit()

    cursor.execute(insert_in_backup)
    print(insert_in_backup)
    db_connection.commit()


def truncate_table(table_name_to_truncate):
    truncate = f"TRUNCATE TABLE {table_name_to_truncate}"
    cursor.execute(truncate)
    print(truncate)

    db_connection.commit()


def insert_without_key(table_to_insert, key_name):
    show_columns = f"SHOW COLUMNS FROM {table_to_insert}"
    cursor.execute(show_columns)
    columns = cursor.fetchall()
    columns_name = [columns[0] for columns in columns if columns[0] != key_name]
    columns_name = (", ".join(columns_name))
    insert_in_backup_table = f"INSERT INTO {table_to_insert} ({columns_name}) SELECT {columns_name} FROM {table_to_insert}_{DATE}"
    cursor.execute(insert_in_backup_table)
    print(insert_in_backup_table)


def set_primary_key(table_name_to_alter, expected_keys, current_keys):
    print(f"\nTable {table_name_to_alter}")
    print('Key', current_keys)
    print('Expected', expected_keys)

    if len(current_keys) > 0:
        # If there are already some primary keys, remove them
        delete_primary_key_constraint = f"ALTER TABLE {table_name_to_alter} DROP PRIMARY KEY"
        print(delete_primary_key_constraint)
        cursor.execute(delete_primary_key_constraint)

    # Add primary keys as a compound key if there are more than one
    key_names = [key['column_name'] for key in expected_keys]
    add_primary_key = f"ALTER TABLE {table_name_to_alter} ADD PRIMARY KEY ({", ".join(key_names)})"
    print(add_primary_key)

    try:
        cursor.execute(add_primary_key)
        # Apply the auto increment for the respective keys
        for key in expected_keys:
            if key['is_autoincrement']:
                add_autoincrement = f"ALTER TABLE {table_name_to_alter} MODIFY COLUMN {key['column_name']} INT AUTO_INCREMENT;"
                print(add_autoincrement)
                cursor.execute(add_autoincrement)
    except Exception as exception1:
        # if table_name in can_truncate:
        #     try:
        #         backup_table(table_name)
        #         truncate_table(table_name)
        #         # Retry adding the primary key
        #         cursor.execute(add_primary_key)
        #         print(add_primary_key)
        #         db_connection.commit()
        #     except Exception as backup_error:
        #         db_connection.rollback()
        #         raise Exception(f"Failed to handle primary key addition: {backup_error}") from e

        if len(expected_keys) == 1 and expected_keys[0]['is_autoincrement']:
            try:
                backup_table(table_name_to_alter)
                truncate_table(table_name_to_alter)

                print(add_primary_key)
                cursor.execute(add_primary_key)

                add_autoincrement = f"ALTER TABLE {table_name_to_alter} MODIFY COLUMN {expected_keys[0]['column_name']} INT AUTO_INCREMENT;"
                print(add_autoincrement)
                cursor.execute(add_autoincrement)

                insert_without_key(table_name_to_alter, expected_keys[0]['column_name'])

            except Exception as backup_error:
                db_connection.rollback()
                raise Exception(f"Failed to handle primary key addition: {backup_error}") from exception1
        else:
            raise Exception(f"Primary key addition failed for {table_name_to_alter}: {exception1}")

    cursor.execute(f"SHOW KEYS FROM {table_name_to_alter} WHERE Key_name = 'PRIMARY'")
    new_schema = cursor.fetchall()
    print('Updated schema', new_schema)


# Step 2: Loop through each table and check for primary keys
for table in tables:
    table_name = table[0]
    if table_name in schema.keys() and table_name not in can_skip:
        expected_schema = schema[table_name]
        cursor.execute(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'")
        actual_schema = cursor.fetchall()

        is_okay = (len(expected_schema) == len(actual_schema))

        try:
            if not is_okay:
                set_primary_key(table_name, expected_schema, actual_schema)

        except Exception as e:
            print('Something went wrong while updating', table_name, e)
            failed.append({'table': table_name, 'error': str(e)})

print(failed)
db_connection.commit()
cursor.close()
db_connection.close()
