import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

# Replace with your PostgreSQL connection details
db_params = {
    "host": "",
    "database": "",
    "user": "",
    "password": ""
}

# Replace with your SMTP server details
smtp_params = {
    "server": "smtp.gmail.com",
    "port": 587,
    "username": "",
    "password": "",
    "sender_email": "",
    "recipient_emails": [
    ]
}

# Connect to the database
connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

# Query to retrieve query information
query = """
SELECT
    pg_stat_activity.pid AS pid,
    pg_stat_activity.query_start AS start_time,
    now() AS current_time,
    now() - pg_stat_activity.query_start AS duration,
    pg_stat_activity.query AS query_text,
    pg_stat_activity.datname AS database_name,
    pg_stat_activity.usename AS username
FROM
    pg_stat_activity
WHERE
    pg_stat_activity.state = 'active'
    AND pg_stat_activity.query != '<IDLE>'
"""

cursor.execute(query)
query_results = cursor.fetchall()

# Prepare HTML email content
html_content = "<html><body>"
html_content += "<h1>ALERT: PG-NRT Query Running From Past 5 min</h1>"
html_content += "<table border='1'><tr><th>PID</th><th>Database Name</th><th>Username</th><th>Start Time</th><th>Current Time</th><th>Duration (seconds)</th><th>Query</th></tr>"

for result in query_results:
    pid, start_time, current_time, duration, query_text, database_name, username = result

    # Calculate the duration in seconds
    duration_seconds = duration.total_seconds()

    # Ensure that end time is greater than start time
    if duration_seconds > 300:  # 5 minutes in seconds
        # Format the start time without microseconds
        start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')

        html_content += f"<tr><td>{pid}</td><td>{database_name}</td><td>{username}</td><td>{start_time}</td><td>{current_time}</td><td>{duration_seconds:.2f}</td><td>{query_text}</td></tr>"

html_content += "</table></body></html>"

# Close the cursor and connection
cursor.close()
connection.close()

# Send HTML email alert to multiple recipients
if "<tr><td>" in html_content:  # Check if any valid query met the condition
    message = MIMEMultipart("alternative")
    message["Subject"] = "PG-NRT Long-Running Query Alert"
    message["From"] = smtp_params["sender_email"]
    message["To"] = ", ".join(smtp_params["recipient_emails"])  # Convert recipient_emails list to a comma-separated string

    # Attach HTML content
    html_part = MIMEText(html_content, "html")
    message.attach(html_part)

    # Send email
    smtp_server = smtplib.SMTP(smtp_params["server"], smtp_params["port"])
    smtp_server.starttls()
    smtp_server.login(smtp_params["username"], smtp_params["password"])
    smtp_server.sendmail(smtp_params["sender_email"], smtp_params["recipient_emails"], message.as_string())
    smtp_server.quit()
    print("Alert sent to {0}".format(", ".join(smtp_params["recipient_emails"])))
