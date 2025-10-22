#!/usr/bin/env python3
import os
import json
import time
import glob
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

DATA_DIR = os.getenv("DATA_DIR", "data_saved")

def get_subscribers():
    """Get list of subscribers"""
    sub_file = os.path.join(DATA_DIR, "subscribers.json")
    if not os.path.exists(sub_file):
        return []
    try:
        with open(sub_file, 'r') as f:
            return json.load(f)
    except:
        return []

def update_last_sent(email, timestamp):
    """Update last sent timestamp for subscriber"""
    sub_file = os.path.join(DATA_DIR, "subscribers.json")
    subscribers = get_subscribers()
    for sub in subscribers:
        if sub['email'] == email:
            sub['last_sent'] = timestamp
    with open(sub_file, 'w') as f:
        json.dump(subscribers, f, indent=2)

def send_alert_email(receiver_email, analysis_content, timestamp):
    """Send alert email with analysis"""
    sender_email = "universalcachetune@gmail.com"
    sender_password = os.environ.get('senderPassword')

    if not sender_password:
        print("Sender password not set.")
        return False

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = f"High Impact Stock Alert - {timestamp}"

    body = f"""
High Impact Stock Alert

Timestamp: {timestamp}

Analysis:
{analysis_content}

This alert was generated based on recent news analysis.
---
To unsubscribe or modify your settings, please contact the administrator.
"""

    msg.attach(MIMEText(body, 'plain'))

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        print(f"Alert email sent to {receiver_email}")
        return True
    except Exception as e:
        print(f"Failed to send alert email to {receiver_email}: {e}")
        return False

def process_analysis(analysis_text, timestamp):
    """Parse analysis JSON and send alerts to eligible subscribers"""
    try:
        # Try to extract JSON from the text (in case there's extra text around it)
        json_start = analysis_text.find('{')
        json_end = analysis_text.rfind('}') + 1

        if json_start != -1 and json_end > json_start:
            json_text = analysis_text[json_start:json_end]
            analysis_data = json.loads(json_text)
        else:
            analysis_data = json.loads(analysis_text)

        potential_impacts = analysis_data.get('potential_impacts', [])
        current_time = time.time()

        subscribers = get_subscribers()
        for sub in subscribers:
            # Check if enough time has passed since last send
            time_since_last = current_time - sub['last_sent']
            if time_since_last < sub['frequency'] * 3600:
                continue

            # Check if any impact meets threshold
            for impact in potential_impacts:
                if impact.get('likelihood', 0) >= sub['threshold']:
                    if send_alert_email(sub['email'], analysis_text, timestamp):
                        update_last_sent(sub['email'], current_time)
                    break

    except json.JSONDecodeError as e:
        print(f"Failed to parse analysis as JSON: {e}")
        print(f"Analysis text: {analysis_text[:200]}...")
    except Exception as e:
        print(f"Error processing analysis: {e}")

def main():
    """Main cron job function - runs continuously"""
    print(f"Starting cron job service at {datetime.now()}")

    while True:
        try:
            print(f"Running cron job cycle at {datetime.now()}")

            # Get latest analysis files
            analysis_files = glob.glob(os.path.join(DATA_DIR, "analysis_*.txt"))
            analysis_files.sort(reverse=True)

            if not analysis_files:
                print("No analysis files found, waiting...")
            else:
                # Process each analysis file that hasn't been processed yet
                processed_count = 0
                for file_path in analysis_files:
                    filename = os.path.basename(file_path)
                    timestamp_str = filename.replace('analysis_', '').replace('.txt', '')

                    try:
                        timestamp = datetime.fromtimestamp(int(timestamp_str)).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        timestamp = filename

                    # Check if already processed
                    processed_file = os.path.join(DATA_DIR, f"processed_{filename}")
                    if os.path.exists(processed_file):
                        continue

                    # Read and process analysis
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()

                        process_analysis(content, timestamp)
                        processed_count += 1

                        # Mark as processed
                        with open(processed_file, 'w') as f:
                            f.write(str(time.time()))

                    except Exception as e:
                        print(f"Error processing file {filename}: {e}")
                        continue

                print(f"Processed {processed_count} new analysis files in this cycle")

            # Wait for 1 hour before next cycle
            print(f"Sleeping for 1 hour before next cycle...")
            time.sleep(3600)  # 1 hour in seconds

        except KeyboardInterrupt:
            print(f"\nStopping cron job service at {datetime.now()}")
            break
        except Exception as e:
            print(f"Error in cron job cycle: {e}")
            print("Retrying in 5 minutes...")
            time.sleep(300)  # 5 minutes on error

if __name__ == "__main__":
    main()
