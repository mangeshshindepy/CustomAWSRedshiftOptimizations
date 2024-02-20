import psycopg2
import boto3
import datetime
import csv
from io import StringIO


class DataTransfer:
    def __init__(self, redshift_config, postgres_config):
        self.redshift_config = redshift_config
        self.postgres_config = postgres_config

    def execute_redshift_query(self, query):
        # Create a connection to Amazon Redshift
        conn = psycopg2.connect(
            host=self.redshift_config['host'],
            port=self.redshift_config['port'],
            database=self.redshift_config['database_name'],
            user=self.redshift_config['user'],
            password=self.redshift_config['password']
        )

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()

    def fetch_redshift_data(self, query):
        # Create a connection to Amazon Redshift
        conn = psycopg2.connect(
            host=self.redshift_config['host'],
            port=self.redshift_config['port'],
            database=self.redshift_config['database_name'],
            user=self.redshift_config['user'],
            password=self.redshift_config['password']
        )

        cursor = conn.cursor()
        cursor.execute(query)
        redshift_data = cursor.fetchall()
        header = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()

        return redshift_data,header

    def insert_into_postgres(self, query, data):
        # Create a connection to PostgreSQL
        conn = psycopg2.connect(
            dbname=self.postgres_config['database_name'],
            user=self.postgres_config['user'],
            password=self.postgres_config['password'],
            host=self.postgres_config['host'],
            port=self.postgres_config['port']
        )

        cursor = conn.cursor()

        for row in data:
            cursor.execute(query, row)

        conn.commit()
        cursor.close()
        conn.close()

    def upload_to_s3(self, data, s3_bucket, s3_prefix, header):
        # Create a date-based folder name
        now = datetime.datetime.now()
        date_folder = now.strftime("%Y-%m-%d")

        # Define the S3 key
        s3_key = f"{s3_prefix}/{date_folder}/stl_scan.csv"

        # Initialize S3 client
        s3 = boto3.client('s3', aws_access_key_id="", aws_secret_access_key="")

        # Create a CSV file in memory
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)

        # Write the header as the first row
        csv_writer.writerow(header)

        # Write the data rows
        csv_writer.writerows(data)
        csv_buffer.seek(0)

        # Encode the CSV content as bytes
        csv_bytes = csv_buffer.getvalue().encode('utf-8')

        # Upload the CSV to S3
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_bytes)

    def transfer_data(self, redshift_query, postgres_query, s3_bucket, s3_prefix):
        self.execute_redshift_query(redshift_query)
        redshift_data,header = self.fetch_redshift_data(redshift_query)
        # self.insert_into_postgres(postgres_query, redshift_data)
        self.upload_to_s3(redshift_data, s3_bucket, s3_prefix,header)

if __name__ == "__main__":
    # Amazon Redshift and PostgreSQL configurations
    redshift_config = {
        'host': '',
        'port': 5439,
        'database_name': '',
        'user': '',
        'password': '',
    }

    postgres_config = {
        'database_name': 'redshiftdb',
        'user': 'postgres',
        'password': 'Mangesh_17',
        'host': 'localhost',
        'port': 5432
    }

    # S3 bucket information
    s3_bucket = 'nrt-dmi-incremental-ingestion'
    s3_prefix = 'Redshift_Query_Log'  # Customize as needed

    # Instantiate the DataTransfer class
    data_transfer = DataTransfer(redshift_config, postgres_config)

    # Define Redshift and PostgreSQL queries
    redshift_query = """SELECT * FROM stl_scan;"""

    postgres_query = """
        INSERT INTO RedshiftMostUsedTable (database, schemaname, table_id, tablename, size, sortkey1, num_qs, starttime, endtime, created, updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()); 
        """

    # Execute the data transfer
    data_transfer.transfer_data(redshift_query, postgres_query, s3_bucket, s3_prefix)
    print("Successful")
