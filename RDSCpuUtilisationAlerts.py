import boto3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

# AWS credentials and region
# aws_access_key = 'YOUR_ACCESS_KEY'
# aws_secret_key = 'YOUR_SECRET_KEY'
# region_name = 'us-east-1'  # Change to your desired region

# Initialize AWS clients
# cloudwatch = boto3.client('cloudwatch')
cloudwatch = boto3.client('cloudwatch', aws_access_key_id='', aws_secret_access_key='',region_name='')
# cloudwatch = boto3.client('cloudwatch', aws_access_key_id='', aws_secret_access_key='', region_name='')

# RDS DB Instance Identifier
db_instance_identifier = ''

# List of RDS DB Instance Identifiers
db_instance_identifiers = ['payment-db', 'nrt-db', 'nrt-db-reader-node']

# Metrics to fetch
metrics = [
    {
        'MetricName': 'CPUUtilization',
        'Statistic': 'Average',
        'Unit': 'Percent',
        'title': 'RDS CPU Utilization'
    },
    {
        'MetricName': 'DatabaseConnections',
        'Statistic': 'Average',
        'Unit': 'Count',
        'title': 'RDS Database Connections'
    },
    {
        'MetricName': 'ReadIOPS',
        'Statistic': 'Average',
        'Unit': 'Count/Second',
        'title': 'RDS Read IOPS'
    },
    {
        'MetricName': 'WriteIOPS',
        'Statistic': 'Average',
        'Unit': 'Count/Second',
        'title': 'RDS Write IOPS'
    },
    {
        'MetricName': 'BufferCacheHitRatio',
        'Statistic': 'Average',
        'Unit': 'Percent',
        'title': 'RDS Buffer Cache Hit Ratio'
    }
]

# Set the time period for the last 30 days
end_time = datetime.now()
start_time = end_time - timedelta(days=30)

# Send Email with Graphs and Tabular Data
email_subject = f'RDS Account Usage Report || Daily'
email_from = ''
#email_to = ['']
email_to = []

# Initialize the SMTP server
# Initialize the SMTP server
smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_username = ''
smtp_password = ''

# Create a MIME multipart message
msg = MIMEMultipart()
msg['From'] = email_from
msg['Subject'] = email_subject

# Create an HTML email body with the table and graphs
email_body = f"""
<html>
<body>
<h2>RDS Metrics Report</h2>
<p>Please find the RDS metrics report below:</p>
"""

for db_instance_identifier in db_instance_identifiers:
    # Fetch and create DataFrame for each metric
    dfs = []
    for metric in metrics:
        response = cloudwatch.get_metric_data(
            MetricDataQueries=[
                {
                    'Id': 'm1',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/RDS',
                            'MetricName': metric['MetricName'],
                            'Dimensions': [
                                {
                                    'Name': 'DBInstanceIdentifier',
                                    'Value': db_instance_identifier
                                },
                            ]
                        },
                        'Period': 86400,  # 1 day
                        'Stat': metric['Statistic'],
                        'Unit': metric['Unit']
                    },
                    'ReturnData': True,
                },
            ],
            StartTime=start_time,
            EndTime=end_time,
        )

        data = response['MetricDataResults'][0]['Values']
        df = pd.DataFrame(data, columns=[metric['MetricName']])
        df.index = pd.date_range(start=start_time, periods=len(data), freq='D')
        dfs.append(df)

    # Concatenate DataFrames horizontally
    df_combined = pd.concat(dfs, axis=1)

    # Convert DataFrame to HTML table
    email_body += f"""
    <h3>RDS Instance: {db_instance_identifier}</h3>
    """
    #email_body += df_combined.to_html()

    # Attach the graphs as inline images
    for metric in metrics:
        plt.figure(figsize=(10, 6))
        plt.plot(df_combined.index, df_combined[metric['MetricName']], marker='o')
        plt.title(metric['title'] + ' Over the Last 30 Days')
        plt.xlabel('Date')
        plt.ylabel(metric['Unit'])
        plt.grid(True)
        plt.xticks(rotation=45)

        # Save the graph as an image
        graph_image_path = f'{db_instance_identifier}_{metric["MetricName"]}_graph.png'
        plt.savefig(graph_image_path)
        plt.close()

        # Attach the graph image as inline image
        with open(graph_image_path, 'rb') as img_file:
            img_content = img_file.read()
            img = MIMEImage(img_content)
            img.add_header('Content-ID', f'<{db_instance_identifier}_{metric["MetricName"]}_graph>')
            msg.attach(img)

        # Add the reference to the inline image in the email body
        email_body += f"""
        <p>{metric['title']}:</p>
        <img src="cid:{db_instance_identifier}_{metric["MetricName"]}_graph"><br><br>
        """

# Complete the email body
email_body += """
</body>
</html>
"""

# Attach the HTML email body
msg.attach(MIMEText(email_body, 'html'))

# Connect to the SMTP server and send the email
with smtplib.SMTP(smtp_server, smtp_port) as server:
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(email_from, email_to, msg.as_string())

print(f'Email sent successfully to {", ".join(email_to)}!')
