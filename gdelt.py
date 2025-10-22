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

# Configuration
BASE = "http://data.gdeltproject.org/gdeltv3/gqg/{stamp}.gqg.json.gz"
KEYWORDS = ["trump", "china", "tariff", "LLM", "data center", "NVidia", "OpenAI"]
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
DATA_DIR = os.getenv("DATA_DIR", "data_saved")
INGEST_COMMAND = ["python", "-m", "synthetic_data_kit.cli", "ingest", "--output-dir", DATA_DIR]

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
    """Download a gz file with light retries. Returns None on 404/absent minute."""
    for attempt in range(retries + 1):
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
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
    for record in iter_gqg_minutes(start_time, end_time):
        # Filter for English
        if record.get('lang') != 'eng':
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
    return articles



def ingest_article(url):
    """Ingest article using synthetic-data-kit and return the full text."""
    try:
        cmd = INGEST_COMMAND + [url]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.getcwd())
        if result.returncode == 0:
            print(f"Successfully ingested {url}")
            # Parse output_path from stdout
            match = re.search(r"Text successfully extracted to \[bold\](.*?)\[/bold\]", result.stdout)
            if match:
                output_path = match.group(1)
                # Read the full text from the file
                with open(output_path, 'r', encoding='utf-8') as f:
                    full_text = f.read()
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

def send_to_openrouter(feeds):
    """Send feeds to OpenRouter for analysis with two-step process."""
    openrouter_key = get_openrouter_key()
    if not openrouter_key:
        print("OpenRouter API key not found.")
        return

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

    prompt1 = f"Here is a list of articles:\n{articles_list}\n\nWhich ones are most relevant in answering this prompt: 'Historically, how has news like this impacted the stock market? Which Stocks will this impact the most? Give a score 1(min) - 10(max) on how likely there will be impact.'? If none are relevant, say 'abort' or 'no relevant news'. If you want full text for some, say 'yes' and provide the article IDs in order of relevance like 1, 2, 3."

    payload1 = {
        "model": "tngtech/deepseek-r1t2-chimera:free",
        "messages": [
            {"role": "user", "content": prompt1}
        ]
    }

    try:
        response1 = requests.post(OPENROUTER_API_URL, headers=headers, json=payload1)
        response1.raise_for_status()
        result1 = response1.json()
        response_text = result1['choices'][0]['message']['content']
        print("First OpenRouter Response:", response_text)

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
                prompt_base2 = "Historically, how has news like this impacted the stock market? Which Stocks will this impact the most? Give a score 1(min) - 10(max) on how likely there will be impact.\n\nRelevant Articles:\n"
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

                response2 = requests.post(OPENROUTER_API_URL, headers=headers, json=payload2)
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
            else:
                print("No valid IDs found in response.")
        else:
            print("No full text requested, using descriptions.")
            # Use descriptions for all
            feeds_text = "\n\n".join([f"Title: {feed.get('title', '')}\nDescription: {feed.get('description', '')}" for feed in feeds_sorted])
            prompt2 = f"Historically, how has news like this impacted the stock market? Which Stocks will this impact the most? Give a score 1(min) - 10(max) on how likely there will be impact.\n\nArticles:\n{feeds_text}"

            payload2 = {
                "model": "tngtech/deepseek-r1t2-chimera:free",
                "messages": [
                    {"role": "user", "content": prompt2}
                ]
            }

            response2 = requests.post(OPENROUTER_API_URL, headers=headers, json=payload2)
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
    except requests.RequestException as e:
        print(f"Error sending to OpenRouter: {e}")

async def main():
    """Main loop to run every minute."""
    while True:
        print(f"Querying GDELT at {datetime.now()}")
        articles = await query_gdelt()

        if articles:
            print(f"Found {len(articles)} relevant articles.")
            feeds = []
            for article in articles:
                url = article.get('url')
                if url:
                    full_text = ingest_article(url)
                    if full_text:
                        save_text_locally(url, full_text)
                        # Update article with full_text for feeds
                        article['full_text'] = full_text
                        feeds.append(article)
            # Send to OpenRouter
            send_to_openrouter(feeds)
        else:
            print("No relevant articles found.")

        await asyncio.sleep(60)  # Wait for 1 minute

if __name__ == "__main__":
    asyncio.run(main())
