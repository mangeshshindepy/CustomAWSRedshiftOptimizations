import psycopg2
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd

class RedshiftQueryExecutor:
    def __init__(self, dbname, user, password, host, port):
        self.db_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }

    def connect(self):
        self.conn = psycopg2.connect(**self.db_params)
        self.cur = self.conn.cursor()

    def disconnect(self):
        self.cur.close()
        self.conn.close()

    def execute_query(self, query):
        self.cur.execute(query)
        query_results = self.cur.fetchall()
        return query_results

def send_email(subject, plain_text_body, html_body):
    # Define your SMTP server settings
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    smtp_username = ''
    smtp_password = ''
    sender_email = ''
    recipient_emails = []

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
    dbname = ''
    user = ''
    password = ''
    host = ''
    port = ''

    executor = RedshiftQueryExecutor(dbname, user, password, host, port)
    executor.connect()

    query =  """
            SELECT
    TRIM(u.usename) AS user,
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
    SUBSTRING(REPLACE(NVL(qrytext_cur.text, TRIM(TRANSLATE(s.text, CHR(10) || CHR(13) || CHR(9), ''))), '\\n', ' '), 1, 90) AS sql,
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
ORDER BY q.service_class, q.exec_time DESC, q.wlm_start_time; 
            """

    query_results = executor.execute_query(query)

    # Initialize an empty list to store alerts
    alerts = []

    try:
        for result in query_results:
            exe_sec = result[9]  # Assuming exe_sec is in the 10th column (0-based index)
            if exe_sec >900:  # Check if execution time exceeds 900 seconds
                # Create an alert message for this result
                alert_message_plain_text = f"Alert: Redshift Query Execution time exceeded 900 seconds.\n"
                alert_message_plain_text += f"Alert Generatation Timestamp: {datetime.now()}\n"

                # Add the code to create a DataFrame from the query results
                column_names = [desc[0] for desc in executor.cur.description]
                df = pd.DataFrame([result], columns=column_names)

                # Include the DataFrame as a table in the email
                alert_message_html = f"<html><body><h2>Alert: Redshift Query Execution time exceeded 900 seconds (15 min).</h2>"
                alert_message_html += f"<p><b>Alert Generatation Timestamp:</b> {datetime.now()}</p>"

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
    finally:
        executor.disconnect()

if __name__ == "__main__":
    main()
