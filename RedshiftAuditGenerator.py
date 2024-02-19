import psycopg2
import boto3
import csv
import smtplib
from email.mime.multipart import MIMEMultipart
from io import StringIO
from email.mime.text import MIMEText
from datetime import datetime
import pandas as pd


# Redshift and S3 connection parameters
redshift_params = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': '',
}

postgres_params = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': '',
}

s3_params = {
    'bucket': '',
    'prefix': 'Redshift_AuditLogs',
}

redshift_query = """
SELECT
    TRIM(u.usename) AS username,
    s.pid,
    q.xid,
    q.query,
    q.service_class AS "q",
    q.slot_count AS slt,
    DATE_TRUNC('second', q.wlm_start_time) AS start,
    TRIM(CASE
        WHEN q.state = 'Running' THEN 'Run'
        WHEN q.state = 'QueuedWaiting' THEN 'Queue'
        ELSE 'Other'
    END) AS state,
    q.queue_Time / 1000000 AS q_sec,
    q.exec_time / 1000000 AS exe_sec,
    m.cpu_time / 1000000 AS cpu_sec,
    m.blocks_read AS read_mb,
    DECODE(m.blocks_to_disk, -1, NULL, m.blocks_to_disk) AS spill_mb,
    m2.rows AS ret_rows,
    m3.rows AS NL_rows,
    REPLACE(NVL(qrytext_cur.text, TRIM(TRANSLATE(s.text, CHR(10) || CHR(13) || CHR(9), ''))), '\\n', ' ') AS sql,
    TRIM(
        DECODE(
            event & 1, 1, 'SK ',
            ''
        ) ||
        DECODE(
            event & 2, 2, 'Del ',
            ''
        ) ||
        DECODE(
            event & 4, 4, 'NL ',
            ''
        ) ||
        DECODE(
            event & 8, 8, 'Dist ',
            ''
        ) ||
        DECODE(
            event & 16, 16, 'Bcast ',
            ''
        ) ||
        DECODE(
            event & 32, 32, 'Stats ',
            ''
        )
    ) AS Alert
FROM stv_wlm_query_state q
LEFT OUTER JOIN stl_querytext s ON (s.query = q.query AND sequence = 0)
LEFT OUTER JOIN stv_query_metrics m ON (q.query = m.query AND m.segment = -1 AND m.step = -1)
LEFT OUTER JOIN stv_query_metrics m2 ON (q.query = m2.query AND m2.step_type = 38)
LEFT OUTER JOIN (
    SELECT query, SUM(rows) AS rows FROM stv_query_metrics m3 WHERE step_type = 15 GROUP BY 1
) AS m3 ON (q.query = m3.query)
LEFT OUTER JOIN pg_user u ON (s.userid = u.usesysid)
LEFT OUTER JOIN (
    SELECT ut.xid, 'CURSOR ' || TRIM(SUBSTRING(TEXT FROM STRPOS(UPPER(TEXT), 'SELECT'))) AS TEXT
    FROM stl_utilitytext ut
    WHERE sequence = 0
    AND UPPER(TEXT) LIKE 'DECLARE%'
    GROUP BY TEXT, ut.xid
) qrytext_cur ON (q.xid = qrytext_cur.xid)
LEFT OUTER JOIN (
    SELECT query, SUM(DECODE(TRIM(SPLIT_PART(event, ':', 1)), 'Very selective query filter', 1, 'Scanned a large number of deleted rows', 2, 'Nested Loop Join in the query plan', 4, 'Distributed a large number of rows across the network', 8, 'Broadcasted a large number of rows across the network', 16, 'Missing query planner statistics', 32, 0)) AS event FROM STL_ALERT_EVENT_LOG
    WHERE event_time >= DATEADD(HOUR, -8, CURRENT_DATE) GROUP BY query
) AS alrt ON alrt.query = q.query
WHERE TRIM(q.state) IN ('Running', 'QueuedWaiting')
ORDER BY q.service_class, q.exec_time DESC, q.wlm_start_time
"""
table_name = 'RedshiftRunningQueriesTable1'

create_table_script = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    username VARCHAR(255),
    pid INT,
    xid INT,
    query TEXT,
    q VARCHAR(255),
    slt INT,
    start TIMESTAMP,
    state VARCHAR(255),
    q_sec NUMERIC,
    exe_sec NUMERIC,
    cpu_sec NUMERIC,
    read_mb NUMERIC,
    spill_mb NUMERIC,
    ret_rows INT,
    NL_rows INT,
    sql TEXT,
    Alert VARCHAR(255),
    PRIMARY KEY (query, start)
);
"""


def execute_redshift_query(redshift_params, redshift_query):
    conn = psycopg2.connect(**redshift_params)
    cursor = conn.cursor()

    cursor.execute(redshift_query)
    result = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()
    print(result)

    return result,column_names

def insert_into_postgres(postgres_params, data, table_name, column_names):
    conn = psycopg2.connect(**postgres_params)
    cursor = conn.cursor()

    # Create the INSERT query dynamically based on the available data columns
    insert_query = f"""
        INSERT INTO {table_name} ({", ".join(f'"{col}"' for col in column_names)})
        VALUES ({", ".join(["%s"] * len(column_names))})
        ON CONFLICT (query, start) DO UPDATE
        SET {", ".join([f'"{col}" = EXCLUDED."{col}"' for col in column_names])};
    """

    cursor.executemany(insert_query, data)
    conn.commit()

    cursor.close()
    conn.close()
def save_result_to_csv(result, output_file_path, column_names):
    with open(output_file_path, 'w', newline='') as file:
        csv_writer = csv.writer(file)
        # Write the column names as the first row
        csv_writer.writerow(column_names)

        for row in result:
            # Encode and decode the SQL column to handle special characters and line breaks
            row = [str(val) if val is not None else '' for val in row]
            row[-1] = row[-1].replace('\n', ' ').replace('\r', ' ')
            csv_writer.writerow(row)

def upload_to_s3(s3_params, local_file_path, s3_key):
    s3_client = boto3.client('s3',aws_access_key_id="", aws_secret_access_key="")
    s3_client.upload_file(local_file_path, s3_params['bucket'], s3_key)

def send_email(subject, plain_text_body, html_body):
    # Define your SMTP server settings
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    smtp_username = ''
    smtp_password = ''
    sender_email = ''
    #recipient_emails = ['mangesh.shinde@dataeaze.io']
    recipient_emails = ['']

    # Create an SMTP connection
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)

    # Create the email message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipient_emails)

    # Create plain text part
    text_part = MIMEText(plain_text_body, 'plain')
    msg.attach(text_part)

    # Create HTML part
    html_part = MIMEText(html_body, 'html')
    msg.attach(html_part)

    # Send the email
    server.sendmail(sender_email, recipient_emails, msg.as_string())
   # print(f"Alert sent sucessfully to {recipient_email}")
    # Close the SMTP connection
    server.quit()


def main():
    # Step 1: Execute the Redshift query
    result,column_names = execute_redshift_query(redshift_params, redshift_query)

    alerts = []

    try:
        # Your existing code for processing the query results and creating alerts
        for res in result:
            exe_sec = res[9]  # Assuming exe_sec is in the 10th column (0-based index)
            if exe_sec >900:  # Check if execution time exceeds 900 seconds
                # Create an alert message for this result
                alert_message_plain_text = f"Alert: Redshift Query Execution time exceeded 900 seconds.\n"
                alert_message_plain_text += f"Alert Generation Timestamp: {datetime.now()}\n"

                df = pd.DataFrame([res], columns=column_names)
                print("df",df)

                # Include the DataFrame as a table in the email
                alert_message_html = f"<html><body><h2>Alert: Redshift Query Execution time exceeded 900 seconds (15 min).</h2>"
                alert_message_html += f"<p><b>Alert Generation Timestamp:</b> {datetime.now()}</p>"

                # Convert DataFrame to HTML table
                alert_message_html += df.to_html(index=False)

                alert_message_html += "</body></html>"

                # Add the alert message to the list of alerts
                alerts.append((alert_message_plain_text, alert_message_html))

        # Check if there are any alerts to send
        if alerts:
            # Send a single email containing all the alerts
            combined_plain_text = "\n\n".join([plain_text for plain_text, _ in alerts])
            combined_html = "\n\n".join([html for _, html in alerts])
            send_email('Alert: Long Running Redshift Queries', combined_plain_text, combined_html)


    except Exception as e:
        print(f"Error: Unable to execute the query or send the alert - {e}")

    # Step 2: Save the result to a local CSV file
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_file_path = f"redshift_output_{timestamp}.csv"
    save_result_to_csv(result, output_file_path, column_names)

    # Step 3: Upload the CSV file to S3 with date-wise partitioning
    today = datetime.now().strftime("%Y_%m_%d")
    s3_key = f"{s3_params['prefix']}/{today}/redshift_output_{timestamp}.csv"
    upload_to_s3(s3_params, output_file_path, s3_key)

    print(f"Data saved to S3: s3://{s3_params['bucket']}/{s3_key}")

    conn = psycopg2.connect(**postgres_params)
    cursor = conn.cursor()
    cursor.execute(create_table_script)
    conn.commit()
    cursor.close()
    conn.close()
    insert_into_postgres(postgres_params, result, table_name,column_names)
    print(f"Inserted into postgres {table_name}")

if __name__ == "__main__":
    main()

