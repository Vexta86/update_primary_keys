import mysql.connector

from modify_table import set_primary_key, insert_without_key
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

DATE = '0124'

schema = json_to_dict('schema_carlos_vieco.json')

# Step 1: Get a list of tables
cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db_name}';")
tables = cursor.fetchall()

failed = []
can_truncate = ('Periodo1', 'Registro_comportamental4', 'Alimentacion', 'Alerta_academica4', 'Instituciones', 'Seguimiento3', 'Seguimiento1', 'Seguimiento2', 'Periodo3', 'Periodo2', 'Registro_comportamental3', 'Registro_comportamental1', 'EstudianteELIM', 'Periodo0', 'Periodo4', 'Alerta_academica3', 'Registro_comportamental2', 'Alerta_academica1', 'Seguimiento4', 'Alerta_academica2')

can_skip = ('Promedio_puesto')

for table in tables:
    table_name = table[0]
    if table_name in schema.keys() and table_name not in can_skip:
        expected_schema = schema[table_name]
        cursor.execute(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'")
        actual_schema = cursor.fetchall()

        is_okay = (len(expected_schema) == len(actual_schema))

        try:
            if not is_okay:
                if set_primary_key(cursor, db_connection, table_name, expected_schema, actual_schema, DATE, can_truncate):
                    print('Successfully updated')

        except Exception as e:
            print('Something went wrong while updating', table_name, e)
            failed.append({'table': table_name, 'error': str(e)})


print(failed)
print([fail['table'] for fail in failed])
db_connection.commit()
cursor.close()
db_connection.close()
