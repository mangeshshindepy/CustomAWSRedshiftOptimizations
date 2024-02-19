import boto3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

# Initialize AWS client
#cloudwatch = boto3.client('cloudwatch',)
cloudwatch = boto3.client('cloudwatch', aws_access_key_id='', aws_secret_access_key='',region_name='ap-south-1')
# Redshift Cluster Identifier
cluster_identifier = ''

# Metrics to fetch
metrics = [
    {
        'MetricName': 'CPUUtilization',
        'Statistic': 'Average',
        'Unit': 'Percent',
        'title': 'Redshift CPU Utilization'
    },
    {
        'MetricName': 'PercentageDiskSpaceUsed',
        'Statistic': 'Average',
        'Unit': 'Percent',
        'title': 'Redshift Percentage Disk Space Used'
    },
    {
        'MetricName': 'DatabaseConnections',
        'Statistic': 'Average',
        'Unit': 'Count',
        'title': 'Redshift Database Connections'
    }
]

# Set the time period for the last 30 days
end_time = datetime.now()
start_time = end_time - timedelta(days=30)

# Fetch metrics and create tabular result
data = []

for metric in metrics:
    response = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'm1',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/Redshift',
                        'MetricName': metric['MetricName'],
                        'Dimensions': [
                            {
                                'Name': 'ClusterIdentifier',
                                'Value': cluster_identifier
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

    metric_data = response['MetricDataResults'][0]['Values']
    for index, value in enumerate(metric_data):
        data.append([start_time + timedelta(days=index), metric['MetricName'], value])

# Create a DataFrame
df = pd.DataFrame(data, columns=['Date', 'Metric', 'Value'])

# Pivot the DataFrame for desired output format
df_pivot = df.pivot_table(index='Date', columns='Metric', values='Value', aggfunc='first')

# Send Email with Graphs and Tabular Data
email_subject = 'REDSHIFT Account Usage Report || Daily'
email_from = ''
#email_to = ['mangesh.shinde@dataeaze.io']#
email_to = []
email_body = 'Please find the Redshift metrics report below.\n\n'

# Initialize the SMTP server
smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_username = ''
smtp_password = ''

# Create a MIME multipart message
msg = MIMEMultipart()
msg['From'] = email_from
msg['To'] = ', '.join(email_to)  # Join recipients with comma
msg['Subject'] = email_subject

# Create an HTML email body
email_body = f"""
<html>
<body>
<h2>Redshift Metrics Report</h2>
<p>Please find the Redshift metrics report below:</p>
"""

# Attach the tabular data as HTML
#email_body += df_pivot.to_html()

# Embed the graphs as inline images and add them to the email body
for metric in metrics:
    plt.figure(figsize=(10, 6))
    plt.plot(df_pivot.index, df_pivot[metric['MetricName']], marker='o')
    plt.title(metric['title'] + ' Over the Last 30 Days')
    plt.xlabel('Date')
    plt.ylabel(metric['Unit'])
    plt.grid(True)
    plt.xticks(rotation=45)

    # Save the graph as an image
    graph_image_path = f'{metric["MetricName"]}_graph.png'
    plt.savefig(graph_image_path)
    plt.close()

    # Attach the graph image as inline image
    with open(graph_image_path, 'rb') as img_file:
        img_content = img_file.read()
        img = MIMEImage(img_content)
        img.add_header('Content-ID', f'<{metric["MetricName"]}_graph>')
        msg.attach(img)

    # Add the reference to the inline image in the email body
    email_body += f"""
    <p>{metric['title']}:</p>
    <img src="cid:{metric['MetricName']}_graph"><br><br>
    """

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
