import asyncio
import os
import requests
import json
import subprocess
import time
import re
import gzip
import io
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Iterator, Dict, Optional, List
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configuration
BASE = "http://data.gdeltproject.org/gdeltv3/gqg/{stamp}.gqg.json.gz"
KEYWORDS = ["trump", "china", "tariff", "LLM", "data center", "NVidia", "OpenAI"]
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
DATA_DIR = os.getenv("DATA_DIR", "data_saved")
CACHE_DIR = os.path.join(DATA_DIR, "cache")
INGEST_COMMAND = ["python", "-m", "synthetic_data_kit.cli", "ingest", "--output-dir", CACHE_DIR]

def _minute_stamps(start_utc: datetime, end_utc: datetime) -> Iterator[str]:
    """Yield YYYYMMDDHHMMSS stamps (UTC) for each minute in [start, end]."""
    if start_utc.tzinfo is not None:
        start_utc = start_utc.astimezone(timezone.utc).replace(tzinfo=None)
    if end_utc.tzinfo is not None:
        end_utc = end_utc.astimezone(timezone.utc).replace(tzinfo=None)
    cur = start_utc.replace(second=0, microsecond=0)
    end_floor = end_utc.replace(second=0, microsecond=0)
    while cur <= end_floor:
        yield cur.strftime("%Y%m%d%H%M%S")
        cur += timedelta(minutes=1)

def _download_gz(url: str, timeout: int = 30, retries: int = 2, backoff: float = 1.5) -> Optional[bytes]:
    """Download a gz file with light retries, using cache if available. Returns None on 404/absent minute."""
    # Extract stamp from URL
    stamp = url.split('/')[-1].replace('.gqg.json.gz', '')
    cache_path = os.path.join(CACHE_DIR, f"{stamp}.gqg.json.gz")
    
    # Check cache first
    if os.path.exists(cache_path):
        print(f"Loading from cache: {cache_path}")
        with open(cache_path, 'rb') as f:
            return f.read()
    
    # Download if not in cache
    for attempt in range(retries + 1):
        resp = requests.get(url, timeout=timeout)
        print(f"Downloaded {url}: {resp.status_code}")
        if resp.status_code == 200:
            # Save to cache
            os.makedirs(CACHE_DIR, exist_ok=True)
            with open(cache_path, 'wb') as f:
                f.write(resp.content)
            print(f"Saved to cache: {cache_path}")
            return resp.content
        if resp.status_code == 404:
            return None  # file not yet published / gap minute
        if attempt < retries:
            time.sleep(backoff ** attempt)
    resp.raise_for_status()
    return None  # unreachable

def iter_gqg_minutes(start_utc: datetime, end_utc: datetime) -> Iterator[Dict]:
    """
    Stream raw article records from per-minute GQG JSONL files (UTC range).
    Each minute file has one JSON object per line: {"date": "...", "url": "...", "title": "...", "lang": "...", "quotes":[{"pre":...,"quote":...,"post":...}, ...]}
    """
    for stamp in _minute_stamps(start_utc, end_utc):
        url = BASE.format(stamp=stamp)
        blob = _download_gz(url)
        if blob is None:
            continue
        with gzip.GzipFile(fileobj=io.BytesIO(blob)) as gz:
            for line in gz:
                if not line.strip():
                    continue
                yield json.loads(line)



async def query_gdelt(last_minutes=5):
    """Query GDELT GQG for articles in the last 5 minutes containing keywords."""
    end_time = datetime.now(tz=timezone.utc)
    start_time = end_time - timedelta(minutes=last_minutes)

    articles = []
    count = 0
    for record in iter_gqg_minutes(start_time, end_time):
        count += 1
        # Filter for English
        if record.get('lang') != 'ENGLISH':
            print(f"Discarded non-English article: lang={record.get('lang')}, title={record.get('title', '')[:50]}...")
            continue
        # Check for keywords in title or quotes
        title = record.get('title', '').lower()
        quotes_text = ' '.join([q.get('quote', '') for q in record.get('quotes', [])]).lower()
        if any(keyword in title or keyword in quotes_text for keyword in KEYWORDS):
            # Convert to article format
            article = {
                'title': record.get('title', ''),
                'description': ' '.join([q.get('quote', '') for q in record.get('quotes', [])]),
                'url': record.get('url', ''),
                'seendate': record.get('date', '').replace('-', '').replace(':', '').replace('T', '').replace('Z', '')[:14]  # Convert to YYYYMMDDHHMMSS
            }
            articles.append(article)
    print(f"Processed {count} records from GQG API, found {len(articles)} matching articles.")
    return articles



def ingest_article(url):
    """Ingest article using synthetic-data-kit and return the full text, using cache if available."""
    # Check if already ingested
    filename = url.replace('https://', '').replace('http://', '').replace('/', '_') + '.txt'
    cache_dir = os.path.join(CACHE_DIR, "full_text")
    filepath = os.path.join(cache_dir, filename)
    if os.path.exists(filepath):
        print(f"Loading from cache: {filepath}")
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    
    # Ingest if not cached
    try:
        cmd = INGEST_COMMAND + [url]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getcwd(), timeout=30)
        if result.returncode == 0:
            print(f"Successfully ingested {url}")
            # Parse output_path from stdout
            match = re.search(r"Text successfully extracted to \[bold\](.*?)\[/bold\]", result.stdout)
            if match:
                output_path = match.group(1)
                # Read the full text from the file
                with open(output_path, 'r', encoding='utf-8') as f:
                    full_text = f.read()
                # Save to cache
                os.makedirs(cache_dir, exist_ok=True)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(full_text)
                print(f"Saved to cache: {filepath}")
                return full_text
            else:
                print(f"Could not find output path in output: {result.stdout}")
                return None
        else:
            print(f"Failed to ingest {url}: {result.stderr}")
            return None
    except Exception as e:
        print(f"Error ingesting {url}: {e}")
        return None

def save_text_locally(url, text):
    """Save the full text to a local file."""
    os.makedirs(DATA_DIR, exist_ok=True)
    filename = url.replace('https://', '').replace('http://', '').replace('/', '_') + '.txt'
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(text)
    print(f"Saved text to {filepath}")

def get_openrouter_key():
    """Get OpenRouter API key from environment."""
    return os.getenv('openrouterKey')

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

def get_alerted_stocks():
    """Get list of stocks alerted in last 12 hours"""
    alert_file = os.path.join(DATA_DIR, "alerted_stocks.json")
    if not os.path.exists(alert_file):
        return {}
    try:
        with open(alert_file, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_alerted_stock(ticker):
    """Save stock to alerted list with timestamp"""
    alert_file = os.path.join(DATA_DIR, "alerted_stocks.json")
    alerted = get_alerted_stocks()
    alerted[ticker] = time.time()
    with open(alert_file, 'w') as f:
        json.dump(alerted, f)

def clean_old_alerts():
    """Remove alerts older than 12 hours"""
    alert_file = os.path.join(DATA_DIR, "alerted_stocks.json")
    if not os.path.exists(alert_file):
        return
    alerted = get_alerted_stocks()
    current_time = time.time()
    to_remove = [ticker for ticker, ts in alerted.items() if current_time - ts > 12 * 3600]
    for ticker in to_remove:
        del alerted[ticker]
    with open(alert_file, 'w') as f:
        json.dump(alerted, f)

def get_last_email_time():
    """Get timestamp of last email sent"""
    email_file = os.path.join(DATA_DIR, "last_email.json")
    if not os.path.exists(email_file):
        return 0
    try:
        with open(email_file, 'r') as f:
            data = json.load(f)
            return data.get('last_sent', 0)
    except:
        return 0

def update_last_email_time():
    """Update timestamp of last email sent"""
    email_file = os.path.join(DATA_DIR, "last_email.json")
    with open(email_file, 'w') as f:
        json.dump({'last_sent': time.time()}, f)

def process_analysis(analysis_text, timestamp):
    """Parse analysis JSON and send alerts for high impact stocks"""
    try:
        # Check if an hour has passed since last email
        current_time = time.time()
        last_email_time = get_last_email_time()
        if current_time - last_email_time < 3600:  # 1 hour in seconds
            print("Less than 1 hour since last email, skipping alert")
            return

        # Try to extract JSON from the text (in case there's extra text around it)
        json_start = analysis_text.find('{')
        json_end = analysis_text.rfind('}') + 1

        if json_start != -1 and json_end > json_start:
            json_text = analysis_text[json_start:json_end]
            analysis_data = json.loads(json_text)
        else:
            analysis_data = json.loads(analysis_text)

        potential_impacts = analysis_data.get('potential_impacts', [])

        # Check if any impact has high likelihood (>=8)
        for impact in potential_impacts:
            if impact.get('likelihood', 0) >= 8:
                ticker = impact.get('ticker')
                company = impact.get('company')
                likelihood = impact.get('likelihood')
                reason = impact.get('reason')

                if send_alert_email("shaginhekvs@gmail.com", analysis_text, timestamp):
                    update_last_email_time()
                    print(f"High impact alert sent for {ticker} ({company}) with likelihood {likelihood}")
                break

    except json.JSONDecodeError as e:
        print(f"Failed to parse analysis as JSON: {e}")
        print(f"Analysis text: {analysis_text[:200]}...")
    except Exception as e:
        print(f"Error processing analysis: {e}")

def update_last_sent(email, timestamp):
    """Update last sent timestamp for subscriber"""
    sub_file = os.path.join(DATA_DIR, "subscribers.json")
    subscribers = get_subscribers()
    for sub in subscribers:
        if sub['email'] == email:
            sub['last_sent'] = timestamp
    with open(sub_file, 'w') as f:
        json.dump(subscribers, f, indent=2)

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

def send_to_openrouter(feeds):
    """Send feeds to OpenRouter for analysis with two-step process."""
    openrouter_key = get_openrouter_key()
    if not openrouter_key:
        print("OpenRouter API key not found.")
        return

    # Clean old alerts
    clean_old_alerts()

    headers = {
        'Authorization': f'Bearer {openrouter_key}',
        'Content-Type': 'application/json'
    }

    # Sort feeds by most recent (assuming 'seendate' is in YYYYMMDDHHMMSS format)
    def parse_seendate(date_str):
        try:
            return datetime.strptime(date_str, '%Y%m%d%H%M%S')
        except:
            return datetime.min

    feeds_sorted = sorted(feeds, key=lambda x: parse_seendate(x.get('seendate', '')), reverse=True)

    # Assign IDs and prepare list for first call
    for i, feed in enumerate(feeds_sorted):
        feed['id'] = i + 1

    articles_list = "\n".join([f"{feed['id']}. Title: {feed.get('title', '')}\n   Description: {feed.get('description', '')}\n   URL: {feed.get('url', '')}" for feed in feeds_sorted])

    prompt1 = f" In list of articles that follow - Which ones are most relevant in answering this question: 'Historically, how has news like this impacted the stock market, especially any individual stock?\
          If these do not usually impact US Stock market, say 'abort' or 'no relevant news'. If you want full text for some, say 'yes' and provide the article IDs in order of relevance like 1, 2, 3., in follow up question you will get these to help answer better. Here is a list of articles:\n{articles_list}\n\n"

    payload1 = {
        "model": "tngtech/deepseek-r1t2-chimera:free",
        "messages": [
            {"role": "user", "content": prompt1}
        ]
    }

    try:
        print("Sending first request to OpenRouter...")
        # Save payload1 to file
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(os.path.join(DATA_DIR, f"openrouter_request1_{int(time.time())}.json"), 'w') as f:
            json.dump(payload1, f, indent=2)
        print("Payload1:", json.dumps(payload1, indent=2))
        response1 = requests.post(OPENROUTER_API_URL, headers=headers, json=payload1)
        print("Response1 Status Code:", response1.status_code)
        print("Response1 Headers:", dict(response1.headers))
        print("Response1 Text:", response1.text)
        # Save response1 to file
        with open(os.path.join(DATA_DIR, f"openrouter_response1_{int(time.time())}.json"), 'w') as f:
            json.dump({"status_code": response1.status_code, "headers": dict(response1.headers), "text": response1.text}, f, indent=2)
        response1.raise_for_status()
        result1 = response1.json()
        response_text = result1['choices'][0]['message']['content']
        print("First OpenRouter Response:", response_text)
        # Save response_text to file
        with open(os.path.join(DATA_DIR, f"openrouter_response1_text_{int(time.time())}.txt"), 'w') as f:
            f.write(response_text)

        # Parse response for abort or no relevant
        if 'abort' in response_text.lower() or 'no relevant' in response_text.lower():
            print("No relevant articles found by model, skipping second call.")
            return

        # Parse response for 'yes' and IDs
        if 'yes' in response_text.lower():
            ids = re.findall(r'\d+', response_text)
            relevant_ids = [int(id) for id in ids if 1 <= int(id) <= len(feeds_sorted)]
            if relevant_ids:
                # Get full texts for relevant IDs in order
                relevant_feeds = [feeds_sorted[id-1] for id in relevant_ids]

                # Build feeds_text incrementally to stay under 130K
                max_length = 130000
                prompt_base2 = "You are a financial impact analysis expert specializing in market reaction forecasting.\n\nTask:\nGiven a final list of recent news articles, identify all NASDAQ-listed stocks that could potentially experience price movement as a result of the news.\n\nFor each relevant company:\n1. Provide the **ticker symbol** and **company name**.\n2. Assign a **likelihood score (1–10)**:\n   - 1–3 → Low chance of moving the stock.\n   - 4–6 → Moderate chance of influencing the stock.\n   - 7–10 → High likelihood of significant market impact.\n3. Add a **brief justification (1–2 sentences)** based on the article content and historical market behavior.\n\nOutput format (JSON preferred):\n```json\n{\n  \"potential_impacts\": [\n    {\n      \"ticker\": \"AAPL\",\n      \"company\": \"Apple Inc.\",\n      \"likelihood\": 8,\n      \"reason\": \"Strong AI hardware partnership announcement could affect investor outlook on upcoming earnings.\"\n    },\n    {\n      \"ticker\": \"TSLA\",\n      \"company\": \"Tesla Inc.\",\n      \"likelihood\": 5,\n      \"reason\": \"Supply chain comments may raise moderate concern, but not major short-term risk.\"\n    }\n  ],\n  \"summary\": \"Two major tech firms may see significant trading volume shifts.\"\n}\n```\n\nRelevant Articles:\n"
                feeds_text_parts = []
                current_length = len(prompt_base2)

                for feed in relevant_feeds:
                    part = f"Article {feed['id']}:\nTitle: {feed.get('title', '')}\nFull Text: {feed.get('full_text', '')}\n\n"
                    if current_length + len(part) > max_length:
                        break
                    feeds_text_parts.append(part)
                    current_length += len(part)

                feeds_text = ''.join(feeds_text_parts)
                prompt2 = prompt_base2 + feeds_text

                payload2 = {
                    "model": "tngtech/deepseek-r1t2-chimera:free",
                    "messages": [
                        {"role": "user", "content": prompt2}
                    ]
                }

                print("Sending second request to OpenRouter (with full text)...")
                # Save payload2 to file
                with open(os.path.join(DATA_DIR, f"openrouter_request2_{int(time.time())}.json"), 'w') as f:
                    json.dump(payload2, f, indent=2)
                print("Payload2:", json.dumps(payload2, indent=2))
                response2 = requests.post(OPENROUTER_API_URL, headers=headers, json=payload2)
                print("Response2 Status Code:", response2.status_code)
                print("Response2 Headers:", dict(response2.headers))
                print("Response2 Text:", response2.text)
                # Save response2 to file
                with open(os.path.join(DATA_DIR, f"openrouter_response2_{int(time.time())}.json"), 'w') as f:
                    json.dump({"status_code": response2.status_code, "headers": dict(response2.headers), "text": response2.text}, f, indent=2)
                response2.raise_for_status()
                result2 = response2.json()
                analysis = result2['choices'][0]['message']['content']
                print("Second OpenRouter Analysis:", analysis)
                # Save analysis
                os.makedirs(DATA_DIR, exist_ok=True)
                analysis_file = os.path.join(DATA_DIR, f"analysis_{int(time.time())}.txt")
                with open(analysis_file, 'w', encoding='utf-8') as f:
                    f.write(analysis)
                print(f"Saved analysis to {analysis_file}")
                # Process analysis for alerts
                timestamp = datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S')
                process_analysis(analysis, timestamp)
            else:
                print("No valid IDs found in response.")
        else:
            print("No full text requested, using descriptions.")
            # Use descriptions for all
            feeds_text = "\n\n".join([f"Title: {feed.get('title', '')}\nDescription: {feed.get('description', '')}" for feed in feeds_sorted])
            prompt2 = f"You are a financial impact analysis expert specializing in market reaction forecasting.\n\nTask:\nGiven a final list of recent news articles, identify all NASDAQ-listed stocks that could potentially experience price movement as a result of the news.\n\nFor each relevant company:\n1. Provide the **ticker symbol** and **company name**.\n2. Assign a **likelihood score (1–10)**:\n   - 1–3 → Low chance of moving the stock.\n   - 4–6 → Moderate chance of influencing the stock.\n   - 7–10 → High likelihood of significant market impact.\n3. Add a **brief justification (1–2 sentences)** based on the article content and historical market behavior.\n\nOutput format (JSON preferred):\n```json\n{{\n  \"potential_impacts\": [\n    {{\n      \"ticker\": \"AAPL\",\n      \"company\": \"Apple Inc.\",\n      \"likelihood\": 8,\n      \"reason\": \"Strong AI hardware partnership announcement could affect investor outlook on upcoming earnings.\"\n    }},\n    {{\n      \"ticker\": \"TSLA\",\n      \"company\": \"Tesla Inc.\",\n      \"likelihood\": 5,\n      \"reason\": \"Supply chain comments may raise moderate concern, but not major short-term risk.\"\n    }}\n  ],\n  \"summary\": \"Two major tech firms may see significant trading volume shifts.\"\n}}\n```\n\nArticles:\n{feeds_text}"

            payload2 = {
                "model": "tngtech/deepseek-r1t2-chimera:free",
                "messages": [
                    {"role": "user", "content": prompt2}
                ]
            }

            print("Sending second request to OpenRouter (with descriptions)...")
            # Save payload2 to file
            with open(os.path.join(DATA_DIR, f"openrouter_request2_{int(time.time())}.json"), 'w') as f:
                json.dump(payload2, f, indent=2)
            print("Payload2:", json.dumps(payload2, indent=2))
            response2 = requests.post(OPENROUTER_API_URL, headers=headers, json=payload2)
            print("Response2 Status Code:", response2.status_code)
            print("Response2 Headers:", dict(response2.headers))
            print("Response2 Text:", response2.text)
            # Save response2 to file
            with open(os.path.join(DATA_DIR, f"openrouter_response2_{int(time.time())}.json"), 'w') as f:
                json.dump({"status_code": response2.status_code, "headers": dict(response2.headers), "text": response2.text}, f, indent=2)
            response2.raise_for_status()
            result2 = response2.json()
            analysis = result2['choices'][0]['message']['content']
            print("OpenRouter Analysis (with descriptions):", analysis)
            # Save analysis
            os.makedirs(DATA_DIR, exist_ok=True)
            analysis_file = os.path.join(DATA_DIR, f"analysis_{int(time.time())}.txt")
            with open(analysis_file, 'w', encoding='utf-8') as f:
                f.write(analysis)
            print(f"Saved analysis to {analysis_file}")
            # Process analysis for alerts
            timestamp = datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S')
            process_analysis(analysis, timestamp)
    except requests.RequestException as e:
        print(f"Error sending to OpenRouter: {e}")
        error_details = {"error": str(e)}
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error Response Status Code: {e.response.status_code}")
            print(f"Error Response Headers: {dict(e.response.headers)}")
            print(f"Error Response Text: {e.response.text}")
            error_details["status_code"] = e.response.status_code
            error_details["headers"] = dict(e.response.headers)
            error_details["text"] = e.response.text
        # Save error to file
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(os.path.join(DATA_DIR, f"openrouter_error_{int(time.time())}.json"), 'w') as f:
            json.dump(error_details, f, indent=2)

async def main():
    """Main loop to run every minute."""
    while True:
        print(f"Querying GDELT at {datetime.now()}")
        articles = await query_gdelt(last_minutes=30)  # Fetch last 15 minutes

        if articles:
            print(f"Found {len(articles)} relevant articles.")
            feeds = []
            for article in articles:
                url = article.get('url')
                if url:
                    full_text = ingest_article(url)
                    if full_text:
                        # Update article with full_text for feeds
                        article['full_text'] = full_text
                    # Prepare feed with title and quotes combined
                    feed = {
                        'title': article.get('title', ''),
                        'description': article.get('description', ''),
                        'url': url,
                        'seendate': article.get('seendate', ''),
                        'full_text': full_text if full_text else ''
                    }
                    feeds.append(feed)
            # Send to OpenRouter
            send_to_openrouter(feeds)
        else:
            print("No relevant articles found.")

        await asyncio.sleep(60 * 30)  # Wait for 30 minute

if __name__ == "__main__":
    asyncio.run(main())
