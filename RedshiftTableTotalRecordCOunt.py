import psycopg2
from datetime import datetime


redshift_host = ""
redshift_database = ""
redshift_user = ""
redshift_password = ""
redshift_port = "5439"

# PostgreSQL connection parameters
postgres_host = "localhost"
postgres_database = "redshift"
postgres_user = "postgres"
postgres_password = "password"
postgres_port = "5432"

# Schemas to include in the count
schemas_to_include = ["public", "", "", ""]

# Create a connection to Redshift
redshift_conn = psycopg2.connect(
    host=redshift_host,
    database=redshift_database,
    user=redshift_user,
    password=redshift_password,
    port=redshift_port
)

# Create a cursor to execute SQL queries
redshift_cur = redshift_conn.cursor()

# Get a list of schemas in your database
schema_query = "SELECT schema_name FROM information_schema.schemata"
redshift_cur.execute(schema_query)
schemas = redshift_cur.fetchall()

# Create a connection to PostgreSQL
postgres_conn = psycopg2.connect(
    host=postgres_host,
    database=postgres_database,
    user=postgres_user,
    password=postgres_password,
    port=postgres_port
)

# Create a cursor to execute SQL queries for PostgreSQL
postgres_cur = postgres_conn.cursor()

# Loop through schemas and get table counts for specified schemas
for schema in schemas:
    schema_name = schema[0]
    if schema_name in schemas_to_include:
        table_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
        redshift_cur.execute(table_query)
        tables = redshift_cur.fetchall()
        for table in tables:
            table_name = table[0]
            count_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
            redshift_cur.execute(count_query)
            count = redshift_cur.fetchone()[0]

            # Insert the data into the PostgreSQL table
            insert_query = "INSERT INTO schema_table_count (schema_name, table_name, count, updatedts) VALUES (%s, %s, %s, %s)"
            current_timestamp = datetime.now()
            postgres_cur.execute(insert_query, (schema_name, table_name, count, current_timestamp))

# Commit changes to the PostgreSQL database
postgres_conn.commit()

# Close the cursors and the connections
redshift_cur.close()
redshift_conn.close()
postgres_cur.close()
postgres_conn.close()
print("Successfull")
