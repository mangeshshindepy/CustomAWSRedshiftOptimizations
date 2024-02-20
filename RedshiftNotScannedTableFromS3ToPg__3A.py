import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import StringIO

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
print(num_qs_eq_0_df)
num_qs_eq_0_df = num_qs_eq_0_df.sort_values(by='schemaname', ascending=False)
html_table = num_qs_eq_0_df.to_html(col_space="auto")


def send_email(subject, message_body, html_table, recipients):
    # Email configuration
    from_email = ''  # Sender's email address
    smtp_server = 'smtp.gmail.com'  # Your SMTP server (e.g., for Gmail)
    smtp_port = 587  # Your SMTP server's port (e.g., for Gmail)

    # Email credentials
    email_username = ''  # Your email username
    email_password = ''

    # Create an email message
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['Subject'] = subject

    # Attach the HTML email body
    message_body = message_body.decode('utf-8')  # Convert bytes to string
    msg.attach(MIMEText(message_body + '<br>' + html_table, 'html'))

    # Connect to the SMTP server
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(email_username, email_password)

    # Send the email to multiple recipients
    msg['To'] = ', '.join(recipients)  # Combine recipients into a single string
    server.sendmail(from_email, recipients, msg.as_string())

    server.quit()
    print(f"Email Alert is successfully sent to {', '.join(recipients)}")

# List of recipient email addresses
recipient_emails = ['mangesh.shinde@dataeaze.io']

subject = 'Alert: Redshift Tables Which Are Not Scanned:'
message_body = b"""Dear Team,
Please find the list of Redshift tables that have not been scanned From "08-11-2023"::"""
html_table = num_qs_eq_0_df.to_html(col_space="auto",index=False)

send_email(subject, message_body, html_table, recipient_emails)
