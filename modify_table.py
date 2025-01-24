import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def insert_without_key(cursor, table_to_insert, key_name, backup_date):
    """Inserts data from a backup table into the original table, excluding a specific key column.

    :param cursor: The database cursor to execute SQL queries.
    :param table_to_insert: The name of the target table to insert data into.
    :param key_name: The name of the column to exclude from the operation.
    :param backup_date: The date suffix for the backup table (e.g., '0123').

    :return: bool: True if the operation succeeds, False otherwise.
    """
    try:
        # Validate table and column names
        if not table_to_insert.isidentifier() or not key_name.isidentifier():
            raise ValueError("Invalid table or column name provided.")

        # Fetch column names
        show_columns = f"SHOW COLUMNS FROM `{table_to_insert}`"
        cursor.execute(show_columns)
        columns = cursor.fetchall()

        # Exclude the specified key column
        columns_name = [col[0] for col in columns if col[0] != key_name]
        if not columns_name:
            raise ValueError("No columns available for insertion after excluding the key column.")

        # Wrap column names in backticks
        columns_name = ", ".join(f"`{col}`" for col in columns_name)

        # Construct and execute the SQL query
        backup_table_name = f"{table_to_insert}_{backup_date}"
        insert_in_backup_table = (
            f"INSERT INTO `{table_to_insert}` ({columns_name}) "
            f"SELECT {columns_name} FROM `{backup_table_name}`"
        )
        cursor.execute(insert_in_backup_table)
        logging.info(f"Successfully executed: {insert_in_backup_table}")
        return True

    except Exception as e:
        logging.error(f"Error in insert_without_key: {e}")
        return False


def backup_table(cursor, db_connection, original_name, backup_date):
    """
    Creates a backup of a specified table by duplicating its structure and data.

    Args:
        cursor: The database cursor to execute SQL queries.
        db_connection: The active database connection for committing changes.
        original_name (str): The name of the table to back up.
        backup_date (str): The date suffix to append to the backup table name (e.g., '20250123').

    Returns:
        bool: True if the operation succeeds, False otherwise.
    """
    try:
        # Validate the table name
        if not original_name.isidentifier():
            raise ValueError(f"Invalid table name: {original_name}")

        # Construct SQL queries
        create_table_like = f"CREATE TABLE `{original_name}_{backup_date}` LIKE `{original_name}`"
        insert_in_backup = f"INSERT INTO `{original_name}_{backup_date}` SELECT * FROM `{original_name}`"

        # Execute the queries
        cursor.execute(create_table_like)
        logging.info(f"Table structure backed up: {create_table_like}")
        db_connection.commit()

        cursor.execute(insert_in_backup)
        logging.info(f"Table data backed up: {insert_in_backup}")
        db_connection.commit()

        return True
    except Exception as e:
        logging.error(f"Error during backup_table: {e}")
        return False


def truncate_table(cursor, db_connection, table_name_to_truncate):
    """
    Truncates (empties) a specified table, removing all rows while preserving its structure.

    Args:
        cursor: The database cursor to execute SQL queries.
        db_connection: The active database connection for committing changes.
        table_name_to_truncate (str): The name of the table to truncate.

    Returns:
        bool: True if the operation succeeds, False otherwise.
    """
    try:
        # Validate the table name
        if not table_name_to_truncate.isidentifier():
            raise ValueError(f"Invalid table name: {table_name_to_truncate}")

        # Construct and execute the SQL query
        truncate = f"TRUNCATE TABLE `{table_name_to_truncate}`"
        cursor.execute(truncate)
        logging.info(f"Table truncated: {truncate}")
        db_connection.commit()

        return True
    except Exception as e:
        logging.error(f"Error during truncate_table: {e}")
        return False


def set_primary_key(cursor, db_connection, table_name_to_alter, expected_keys, current_keys, backup_date,
                    can_truncate=()):
    """
    Sets or updates the primary key for a specified table. If a primary key exists, it is removed before adding the new key(s).

    Args:
        :param cursor: The database cursor to execute SQL queries.
        :param db_connection: The active database connection for committing changes.
        :param table_name_to_alter: The name of the table to modify.
        :param expected_keys: A list of dictionaries, where each dictionary contains:
            - 'column_name' (str): The name of the column.
            - 'is_autoincrement' (bool): Whether the column should be auto-incremented.
        :param current_keys: The current primary keys of the table.
        :param backup_date: The date suffix for backup operations.
        :param can_truncate: A list with the table names that can be truncated

    Raises:
        Exception: If the operation fails, an exception is raised with details.

    """
    logging.info(f"Processing table: {table_name_to_alter}")
    logging.info(f"Current primary keys: {current_keys}")
    logging.info(f"Expected primary keys: {expected_keys}")

    # Prepare and execute the query to add new primary keys
    key_names = [f"`{key['column_name']}`" for key in expected_keys]
    add_primary_key = f"ALTER TABLE `{table_name_to_alter}` ADD PRIMARY KEY ({', '.join(key_names)})"
    try:
        if len(current_keys) > 0:
            # Remove existing primary key constraint
            delete_primary_key_constraint = f"ALTER TABLE `{table_name_to_alter}` DROP PRIMARY KEY"
            logging.info(f"Executing: {delete_primary_key_constraint}")
            cursor.execute(delete_primary_key_constraint)

        logging.info(f"Executing: {add_primary_key}")
        cursor.execute(add_primary_key)

        # Add auto-increment for specified columns
        for key in expected_keys:
            if key['is_autoincrement']:
                add_autoincrement = (
                    f"ALTER TABLE `{table_name_to_alter}` MODIFY COLUMN `{key['column_name']}` INT AUTO_INCREMENT;"
                )
                logging.info(f"Executing: {add_autoincrement}")
                cursor.execute(add_autoincrement)

    except Exception as exception1:

        # Handle the case where a single auto-increment key is expected
        if len(expected_keys) == 1 and expected_keys[0]['is_autoincrement']:
            try:
                # Backup and truncate the table before retrying
                logging.warning(f"Primary key addition failed. Backing up and truncating table: {table_name_to_alter}")
                backup_table(cursor, db_connection, table_name_to_alter, backup_date)
                truncate_table(cursor, db_connection, table_name_to_alter)

                # Retry adding the primary key
                logging.info(f"Retrying: {add_primary_key}")
                cursor.execute(add_primary_key)

                add_autoincrement = (
                    f"ALTER TABLE `{table_name_to_alter}` MODIFY COLUMN `{expected_keys[0]['column_name']}` INT AUTO_INCREMENT;"
                )
                logging.info(f"Executing: {add_autoincrement}")
                cursor.execute(add_autoincrement)

                # Reinsert data without the primary key
                insert_without_key(cursor, table_name_to_alter, expected_keys[0]['column_name'], backup_date)

            except Exception as backup_error:
                db_connection.rollback()
                logging.error(f"Failed to handle primary key addition: {backup_error}")
                raise Exception(f"Failed to handle primary key addition: {backup_error}") from exception1
        # Handle the case where the information in the table is not important
        elif table_name_to_alter in can_truncate:
            try:
                # Backup and truncate the table before retrying
                logging.warning(
                    f"Primary key addition failed. Backing up and truncating table: {table_name_to_alter}")
                backup_table(cursor, db_connection, table_name_to_alter, backup_date)
                truncate_table(cursor, db_connection, table_name_to_alter)

                # Retry adding the primary key
                logging.info(f"Retrying: {add_primary_key}")
                cursor.execute(add_primary_key)

            except Exception as backup_error:
                db_connection.rollback()
                logging.error(f"Failed to handle primary key addition: {backup_error}")
                raise Exception(f"Failed to handle primary key addition: {backup_error}") from exception1

        else:
            logging.error(f"Primary key addition failed for {table_name_to_alter}: {exception1}")
            raise Exception(f"Primary key addition failed for {table_name_to_alter}: {exception1}")

    # Verify and log the updated schema
    cursor.execute(f"SHOW KEYS FROM `{table_name_to_alter}` WHERE Key_name = 'PRIMARY'")
    new_schema = cursor.fetchall()
    logging.info(f"Updated schema for {table_name_to_alter}: {new_schema}")
