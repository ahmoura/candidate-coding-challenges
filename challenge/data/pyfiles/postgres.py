from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def open_cursor_and_conn():
    conn = connect(
        user = "postgres",
        host = "database",
        password = "perseus"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    return conn, cursor

def open_cursor_and_conn_with_db(db_name):
    conn = connect(
        dbname = db_name,
        user = "postgres",
        host = "database",
        password = "perseus"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    return conn, cursor

def close_cursor_and_conn(cursor, conn):
    cursor.close()
    conn.close()

def create_database(cursor, db_name):
    try:
        cursor.execute("create database " + db_name + ";")
    except:
        print(f"DuplicateDatabase: database \"{db_name}\" already exists. Command Skipped.")
    
 #    f"SELECT 'CREATE DATABASE {db_name}' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '{db_name}')"

def create_bronze_table(t_name, cursor, conn):
    bronze = {
        "courses": "(description varchar(256), id varchar(256) UNIQUE NOT NULL, publishedAt varchar(256), title varchar(256), PRIMARY KEY (id));",
        "users": "(email varchar(256),firstName varchar(256), id varchar(256) UNIQUE NOT NULL, lastName varchar(256), PRIMARY KEY (id));",
        "certificates": "(completedDate varchar(256), course varchar(256) NOT NULL, startDate varchar(256), \"user\" varchar(256) NOT NULL, FOREIGN KEY (\"user\") REFERENCES users (id), FOREIGN KEY (course) REFERENCES courses (id));"
    }

    create_table_string = "CREATE TABLE " + t_name + bronze.get(t_name)

    try:
        cursor.execute(create_table_string)
        conn.commit()
    except:
        print(f"DuplicateTable: relation \"{t_name}\" already exists. Command Skipped.")


def create_silver_table(t_name, cursor, conn):
    silver = {
        "certificates": "(description varchar(256), publishedAt varchar(256), title varchar(256), email varchar(256), firstName varchar(256), lastName varchar(256), completedDate varchar(256), course varchar(256), startDate varchar(256), \"user\" varchar(256));",
    }

    create_table_string = "CREATE TABLE " + t_name + silver.get(t_name)

    try:
        cursor.execute(create_table_string)
        conn.commit()
    except:
        print(f"DuplicateTable: relation \"{t_name}\" already exists. Command Skipped.")

        
def create_gold_table(t_name, cursor, conn):
    gold = {
        "certificates": "(title varchar(256), email varchar(256), firstName varchar(256), lastName varchar(256), completedDate TIMESTAMP,  startDate TIMESTAMP);",
    }

    create_table_string = "CREATE TABLE " + t_name + gold.get(t_name)

    try:
        cursor.execute(create_table_string)
        conn.commit()
    except:
        print(f"DuplicateTable: relation \"{t_name}\" already exists. Command Skipped.")