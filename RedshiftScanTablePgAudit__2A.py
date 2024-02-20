import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import StringIO
import psycopg2


# Your S3 bucket and prefix
s3_bucket = "nrt-dmi-incremental-ingestion"
s3_prefix = "Redshift_Query_Log/"

# Initialize the S3 client
#s3_client = boto3.client('s3')
s3_client = boto3.client('s3',aws_access_key_id="", aws_secret_access_key="")

# Function to list objects in an S3 prefix
def list_s3_objects(bucket, prefix):
    try:
        objects = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
        return objects.get('Contents', [])
    except NoCredentialsError:
        print("AWS credentials not found")

# List objects in the S3 prefix
objects = list_s3_objects(s3_bucket, s3_prefix)

# Initialize DataFrames to store data
data_frames = []

# Process each object (date folder) and create DataFrames
for obj in objects:
    # Get the key (S3 object path)
    key = obj['Key']

    # Check if the object is a CSV file
    if key.endswith('.csv'):
        # Read the CSV file from S3 as bytes
        s3_object = s3_client.get_object(Bucket=s3_bucket, Key=key)
        csv_data_bytes = s3_object['Body'].read()

        # Read the CSV data into a Pandas DataFrame using StringIO
        csv_df = pd.read_csv(StringIO(csv_data_bytes.decode('utf-8')))

        # Store the DataFrame in the list
        data_frames.append(csv_df)

# Combine all DataFrames into a single DataFrame
combined_df = pd.concat(data_frames, ignore_index=True)
print("combined_df",combined_df)

# Separate data based on 'num_qs' values
num_qs_gt_0_df = combined_df[combined_df['num_qs'] > 0].drop_duplicates(subset=['table_id'])
num_qs_eq_0_df = combined_df[combined_df['num_qs'] == 0].drop_duplicates(subset=['table_id'])

# Analysis
total_row_count = combined_df.shape[0]
unique_row_count = combined_df.drop_duplicates().shape[0]
num_qs_gt_0_df_count = num_qs_gt_0_df.shape[0]
num_qs_eq_0_df_count = num_qs_eq_0_df.shape[0]

print(f"Total row count: {total_row_count}")
print(f"Unique row count: {unique_row_count}")
print(f"num_qs_gt_0_df count: {num_qs_gt_0_df_count}")
print(f"num_qs_eq_0_df count: {num_qs_eq_0_df_count}")

# Inner join on 'table_id' between num_qs_gt_0_df and num_qs_eq_0_df
common_df = pd.merge(num_qs_gt_0_df, num_qs_eq_0_df, on='table_id', how='inner')

# common_df will contain the rows with common 'table_id' values
common_table_ids = common_df['table_id'].tolist()
print("Comman ids:", len(common_table_ids))

if common_table_ids:
    print("Common table_ids between num_qs_gt_0_df and num_qs_eq_0_df:")
    print(common_table_ids)
else:
    print("No common table_ids found between num_qs_gt_0_df and num_qs_eq_0_df.")

table_ids_in_gt_0 = set(num_qs_gt_0_df['table_id'])
num_qs_eq_0_df = num_qs_eq_0_df[~num_qs_eq_0_df['table_id'].isin(table_ids_in_gt_0)]
num_qs_eq_0_df = num_qs_eq_0_df.drop(columns=['starttime', 'endtime','size','sortkey1','num_qs'])
num_qs_eq_0_df = num_qs_eq_0_df.reset_index(drop=True)
# print(num_qs_eq_0_df)

html_table = num_qs_eq_0_df.to_html(col_space="auto")

# print(num_qs_gt_0_df.columns)
num_qs_eq_0_df = num_qs_eq_0_df.sort_values(by='schemaname', ascending=False)
print(num_qs_eq_0_df)



conn = psycopg2.connect(
    dbname='redshift',
    user='postgres',
    password='password',
    host='localhost',
    port='5432'
)

cursor = conn.cursor()

create_table_script = """
CREATE TABLE IF NOT EXISTS scantable (
    database VARCHAR(255),
    schemaname VARCHAR(255),
    table_id INT,
    tablename VARCHAR(255),
    size INT,
    sortkey1 VARCHAR(255),
    num_qs INT,
    starttime TIMESTAMP,
    endtime TIMESTAMP,
    updatedts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_owner VARCHAR(255),
    PRIMARY KEY (table_id, starttime, endtime)
);
"""

# Step 4: Execute the CREATE TABLE script
cursor.execute(create_table_script)

# Commit the CREATE TABLE transaction
conn.commit()

# Step 5: Insert or update records into the table using ON CONFLICT
for row in num_qs_gt_0_df.itertuples(index=False):
    insert_query = """
    INSERT INTO scantable (database, schemaname, table_id, tablename, size, sortkey1, num_qs, starttime, endtime, updatedts,table_owner)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
    ON CONFLICT (table_id, starttime, endtime) DO UPDATE
    SET (database, schemaname, tablename, size, sortkey1, num_qs, updatedts,table_owner)
    = (EXCLUDED.database, EXCLUDED.schemaname, EXCLUDED.tablename, EXCLUDED.size, EXCLUDED.sortkey1, EXCLUDED.num_qs, CURRENT_TIMESTAMP,EXCLUDED.table_owner);
    """
    cursor.execute(insert_query, row)

# Commit the INSERT or UPDATE transactions
conn.commit()

def insert_dataframe_into_postgres(df, conn, table_name):
    # Connect to the PostgreSQL database using psycopg2
    # conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Define the CREATE TABLE script with createdts and updatedts columns
    create_table_script = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        database VARCHAR(255),
        schemaname VARCHAR(255),
        table_id INT,
        tablename VARCHAR(255),
        updatedts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        table_owner VARCHAR(255),
        PRIMARY KEY (table_id)
    );
    """

    # Execute the CREATE TABLE script
    cursor.execute(create_table_script)
    conn.commit()


    for row in df.itertuples(index=False):
        insert_query = f"""
            INSERT INTO {table_name} (database, schemaname, table_id, tablename,table_owner)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (table_id) DO UPDATE
            SET (database, schemaname, tablename, updatedts,table_owner)
            = (EXCLUDED.database, EXCLUDED.schemaname, EXCLUDED.tablename, CURRENT_TIMESTAMP, EXCLUDED.table_owner);
            """
        cursor.execute(insert_query, row)


    # Commit the INSERT transactions
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

insert_dataframe_into_postgres(num_qs_eq_0_df, conn, "notscannedtables")
# Close the cursor and connection
cursor.close()
conn.close()
