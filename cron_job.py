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

def send_alert_email(receiver_email, formatted_content, timestamp):
    """Send alert email with formatted content"""
    sender_email = "universalcachetune@gmail.com"
    sender_password = os.environ.get('senderPassword')

    if not sender_password:
        print("Sender password not set.")
        return False

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = f"High Impact Stock Alert - {timestamp}"

    msg.attach(MIMEText(formatted_content, 'plain'))

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

def deduplicate_impacts(impacts):
    """Remove duplicate impacts based on ticker and likelihood score"""
    seen = set()
    unique_impacts = []
    for impact in impacts:
        key = (impact.get('ticker', 'Unknown'), impact.get('likelihood', 0))
        if key not in seen:
            seen.add(key)
            unique_impacts.append(impact)
    return unique_impacts

def collect_alert_impacts(subscriber, analysis_files):
    """Collect all relevant impacts since subscriber's last email"""
    relevant_impacts = []
    last_sent_time = subscriber.get('last_sent', 0)
    current_time = time.time()

    for file_path in analysis_files:
        filename = os.path.basename(file_path)
        timestamp_str = filename.replace('analysis_', '').replace('.txt', '')

        try:
            file_timestamp = int(timestamp_str)
        except:
            continue

        # If last_sent is 0 (never sent), include all files. Otherwise, only files newer than last_sent.
        if last_sent_time == 0 or file_timestamp > last_sent_time:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Extract JSON
                json_start = content.find('{')
                json_end = content.rfind('}') + 1

                if json_start != -1 and json_end > json_start:
                    json_text = content[json_start:json_end]
                    analysis_data = json.loads(json_text)
                else:
                    analysis_data = json.loads(content)

                potential_impacts = analysis_data.get('potential_impacts', [])
                summary = analysis_data.get('summary', 'No summary available')

                # Filter impacts that meet subscriber's threshold
                for impact in potential_impacts:
                    likelihood = impact.get('likelihood', 0)
                    ticker = impact.get('ticker', 'Unknown')

                    if likelihood >= subscriber['threshold']:
                        # Add file timestamp and summary context
                        impact_copy = impact.copy()
                        impact_copy['_file_timestamp'] = file_timestamp
                        impact_copy['_summary'] = summary
                        relevant_impacts.append(impact_copy)

            except Exception as e:
                print(f"Error processing file {filename} for {subscriber['email']}: {e}")
                continue

    return relevant_impacts

def send_consolidated_alert(subscriber, relevant_impacts, latest_timestamp):
    """Send consolidated alert with all accumulated impacts"""
    if not relevant_impacts:
        return False

    # Remove duplicate impacts
    relevant_impacts = deduplicate_impacts(relevant_impacts)

    # Group impacts by time periods for better organization
    impact_groups = {}
    for impact in relevant_impacts:
        file_time = impact.pop('_file_timestamp', 0)
        summary = impact.pop('_summary', 'General analysis')

        time_key = f"{datetime.fromtimestamp(file_time).strftime('%Y-%m-%d %H:%M')} - {summary}"

        if time_key not in impact_groups:
            impact_groups[time_key] = []
        impact_groups[time_key].append(impact)

    # Format the consolidated email
    alert_content = format_consolidated_email(impact_groups, latest_timestamp, subscriber['threshold'])

    if send_alert_email(subscriber['email'], alert_content, latest_timestamp):
        print(f"Sent consolidated alert to {subscriber['email']} with {len(relevant_impacts)} total impacts")
        return True
    return False

def format_consolidated_email(impact_groups, latest_timestamp, threshold):
    """Format consolidated alert email with grouped impacts"""

    body = f"""High Impact Stock Alert Summary
{latest_timestamp}
(Threshold: {threshold}/10 or higher)

This email contains all stock alerts that met your threshold since your last notification:

"""

    total_impacts = sum(len(impacts) for impacts in impact_groups.values())

    for time_summary, impacts in impact_groups.items():
        body += f"""✨ {time_summary}
({len(impacts)} alert{'s' if len(impacts) > 1 else ''})

"""
        for impact in impacts:
            ticker = impact.get('ticker', 'Unknown')
            company = impact.get('company', 'Unknown Company')
            likelihood = impact.get('likelihood', 0)
            reason = impact.get('reason', 'No reason provided')

            body += f"""   📈 {ticker} - {company}
      Likelihood: {likelihood}/10
      💡 {reason}

"""

    body += f"""
Total alerts in this summary: {total_impacts}

This alert was generated based on recent GDELT news analysis and stock impact predictions.
---
To unsubscribe or modify your settings, please visit the dashboard.
"""

    return body

def main():
    """Main cron job function - runs continuously every minute"""
    print(f"Starting cron job service at {datetime.now()}")

    while True:
        try:
            print(f"Running cron job cycle at {datetime.now()}")

            # Get all analysis files (always scan all to check for new subscribers)
            analysis_files = glob.glob(os.path.join(DATA_DIR, "analysis_*.txt"))
            if not analysis_files:
                print("No analysis files found, waiting...")
            else:
                analysis_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)  # Sort by file modification time

                subscribers = get_subscribers()
                current_time = time.time()
                latest_timestamp = datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')

                for subscriber in subscribers:
                    try:
                        # Collect all relevant impacts since their last email
                        relevant_impacts = collect_alert_impacts(subscriber, analysis_files)

                        if relevant_impacts:
                            # Send consolidated alert
                            if send_consolidated_alert(subscriber, relevant_impacts, latest_timestamp):
                                # Update last sent time
                                update_last_sent(subscriber['email'], current_time)
                                print(f"Updated last sent time for {subscriber['email']}")
                        else:
                            print(f"No new alerts for {subscriber['email']} (threshold: {subscriber['threshold']})")

                    except Exception as e:
                        print(f"Error processing subscriber {subscriber['email']}: {e}")
                        continue

            # Wait for 1 minute before next cycle
            print("Sleeping for 1 minute before next cycle...")
            time.sleep(60)  # 1 minute

        except KeyboardInterrupt:
            print(f"\nStopping cron job service at {datetime.now()}")
            break
        except Exception as e:
            print(f"Error in cron job cycle: {e}")
            print("Retrying in 30 seconds...")
            time.sleep(30)  # 30 seconds on error

if __name__ == "__main__":
    main()
