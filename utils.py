import json


def json_to_dict(json_file):
    """
    Convert a JSON file containing database schema information into a dictionary.

    This function reads a JSON file that describes database tables, their primary keys,
    and whether the primary key has the `auto_increment` attribute. It converts this
    information into a dictionary where each table name is a key, and the value is a
    list of dictionaries containing the primary key column and its auto-increment status.

    The JSON file must have the following structure:
    [
        {"table_name": "Actas_aprobacion", "primary_key_column": "Cod_aprobacion", "auto_increment": "auto_increment"},
        {"table_name": "Actas_reuniones", "primary_key_column": "Cod_acta", "auto_increment": ""},
        ...
    ]

    Example output:
    {
        "Actas_aprobacion": [{"column_name": "Cod_aprobacion", "is_autoincrement": True}],
        "Actas_reuniones": [{"column_name": "Cod_acta", "is_autoincrement": False}],
        ...
    }

    Args:
        json_file (str): Path to the JSON file containing the schema information.

    Returns:
        dict: A dictionary representation of the schema with table names as keys
              and lists of primary key details as values.

    Raises:
        FileNotFoundError: If the specified JSON file does not exist.
        json.JSONDecodeError: If the JSON file is not properly formatted.
    """
    # Initialize an empty dictionary to store the schema
    schema_dict = {}

    # Open and load the JSON file
    with open(json_file, 'r') as file:
        schema_array = json.load(file)

    # Iterate through each table in the JSON array
    for table in schema_array:
        # Create a dictionary for the primary key details
        if table['primary_key_column']:
            key_dict = {
                'column_name': table['primary_key_column'],
                'is_autoincrement': table['auto_increment'] == 'auto_increment'
            }

            # Add the primary key details to the corresponding table in the schema
            name = table['table_name']
            if name not in schema_dict:
                schema_dict[str(name)] = [key_dict]
            else:
                schema_dict[name].append(key_dict)

    return schema_dict
