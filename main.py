import mysql.connector

def create_db_connection(user, password, host, database):
    """Create a connection to the MySQL database."""
    # Establish a connection with the MySQL server
    try:
        conn = mysql.connector.connect(user=user, password=password, host=host, database=database)
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        exit()

    """{
         "table_name": "user",
         "columns": [
            {
                "column_name": "age", 
                "type": "JSON",
                "options": ""
                "default": "",
                "unique": True,
                "foreign_key":{
                    "table_name": "xyz",
                    "column_name": "age",
                }
            },
            {
                "column_name": "school", 
                "type": {
                    "":"Integer",
                    "values":""
                }
                "default": "",
                "unique": ""
            },
            {
                "column_name": "salary", 
                "type": "DECIMAL(10, 2)",
                "default": "",
                "unique": ""
            },
            {
                "column_name": "meta_fields", 
                "type": "JSON",
                "unique": ""
            }
         ]
      }
    """

def check_column_in_table(db, column_name, table_name):
    try:
        mycursor = db.cursor(dictionary=True)
        query = f"""select {column_name} from {table_name} limit 1;"""
        mycursor.execute(query)
        val = mycursor.fetchone()
        return True if val else False
    except Exception:
        print(f"{column_name} is a valid column")
        return False

def check_table_in_database(table_name, db):
    try:
        mycursor = db.cursor(dictionary=True)
        query = f"""select * from {table_name} limit 1;"""
        mycursor.execute(query)
        result = mycursor.fetchall()
        return True if result else False
    except Exception as e:
        print("Table not found: \n",str(e))
        return False


def valid_dtype(dtype):
    valid_data_types = [
        "INTEGER", "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT",
        "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC",
        "CHAR", "VARCHAR", "TEXT", "BINARY", "VARBINARY", "BLOB",
        "DATE", "TIME", "DATETIME", "TIMESTAMP", "YEAR",
        "ENUM", "SET", "JSON"
    ]

    return dtype.upper() in valid_data_types

def check_column_validation(data, db, table_name):
    valid_params = ["column_name", "type", "default", "unique", "options"]
    no_default_dtype = ["BLOB", "TEXT", "GEOMETRY", "JSON", "ENUM"]
    return_message = {"message":""}
    query = ""
    if set(data.keys()).issubset(valid_params):
        if not check_column_in_table(db, data.get("column_name"), table_name): # Column name validation
            query+= f""" ADD COLUMN {data.get("column_name")}"""
            if data.get("type") and valid_dtype(data.get("type")): # Checking the type and is it valid or not
                if data.get("type").lower() in ["enum"]: # Special type ENUM for the options
                    if data.get("options") and isinstance(data.get("options"), list):
                        query += f""" ENUM{tuple(data["options"])}"""
                    else:
                        return_message["message"] += f"Options are required for type '{data['type']} and must be in list.'\n"
                else: # If type is not enum
                    query += f""" {data["type"]} """
            else:
                return_message["message"] += f"\nType not provided for column '{data['column_name']}'"
            # If type of under ["BLOB", "TEXT", "GEOMETRY", "JSON"] it cannot have default values
            if data.get("type").upper() in no_default_dtype and data.get("default") not in [None]:
                return_message["message"] += f'\nDefault value "{data.get("default")}" can\'t be used with datatype "{no_default_dtype} for the column {data.get("column_name")}"'
            else:
                if data.get("default") not in [None]:
                    if isinstance(data["default"], str):
                        query += f""" DEFAULT '{data.get("default")}' """
                    else:
                        query += f""" DEFAULT {data.get("default")} """
            if data.get("unique") == True:
                query += """ UNIQUE """
            # Foreign Key still have to added...
            
        else:
            print(18)
            return_message["message"] += f"\nColumn '{data.get('column_name')}' already exists or not provided."
    else:
        print(19)
        return_message["message"] += "\nKeys are not valid."
        
    if query != "":
        return_message["query"] = query
    return return_message

def add_columns_to_table(table_name, data, db):
    try:
        print(data)
        
        if check_table_in_database(table_name, db):
            """Add columns to an existing table in the database."""
            cursor = db.cursor()
            
            query = f"""ALTER TABLE {table_name} """
            for column_data in data:
                print(column_data)
                message = check_column_validation(column_data, db, table_name)
                if len(message["message"]) > 0:
                    print("Error: ", message["message"])
                else:
                    query += message["query"] + ","
            
            # To remove the last comma
            query = query[:-1]
            print(f"Running Query:\n{query}")
            cursor.execute(query)
            db.commit()
            print("Column added!!!")
        else:
            print(f"Check the table name we cannot find it on database \ntable name: {table_name}")
    except Exception as e:
        print(f"""Something went wrong on adding the column on table '{table_name}'\nerror: {e}""")



db = create_db_connection(user="root", password="#Qw1sd2xc", host="localhost", database="zenarate_db_test")

data =  {
        "table_name": "user",
        "columns":[
            {
                "column_name": "new_int", 
                "type": "ENUM",
                "options": ['a','b'], # Error type
                "default": None,
                "unique": True,
            },
            {
                "column_name": "blue", 
                "type": "ENUM",
                "options": ['a','b'],
                "default": "", # wrong default
                "unique": True,
            },
            {
                "column_name": "school", 
                "type":"VARCHAR(220)",
                "default": "", # Correct
                "unique": ""
            },
            {
                "column_name": "salary", 
                "type": "DECIMAL(10, 2)",
                "default": None, # Correct
                "unique": ""
            },
            {
                "column_name": "meta_fields", 
                "type": "JSON",
                "unique": "", # Wrong
                "default": None
            }
        ]
    }

add_columns_to_table(table_name=data["table_name"], data=data["columns"],db=db)