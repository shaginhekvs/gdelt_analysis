import asyncio
import os
import gdelt
from datetime import datetime

# Set test configuration
os.environ['DATA_DIR'] = "data_dir_test"
DATA_DIR = os.getenv("DATA_DIR", "data_saved")
gdelt.DATA_DIR = DATA_DIR
gdelt.CACHE_DIR = os.path.join(DATA_DIR, "cache")
gdelt.KEYWORDS = ["ai"]  # Test with a keyword
gdelt.INGEST_COMMAND = ["python", "-m", "synthetic_data_kit.cli", "ingest", "--output-dir", gdelt.CACHE_DIR]



async def main():
    """Test GDELT API and ingestion."""
    print(f"Querying GDELT at {datetime.now()}")
    articles = await gdelt.query_gdelt(last_minutes=15)  # Test with last 15 minutes

    if articles:
        print(f"Found {len(articles)} articles.")
        feeds = []
        for i, article in enumerate(articles[:3]):  # Test with first 3 articles
            url = article.get('url')
            title = article.get('title', 'No title')
            print(f"Article {i+1}: {title}")
            if url:
                full_text = gdelt.ingest_article(url)
                if full_text:
                    print(f"Full text length: {len(full_text)} characters")
                    # Update article with full_text for feeds
                    article['full_text'] = full_text
                else:
                    print("Failed to ingest article.")
            else:
                print("No URL for article.")
        
        # Prepare feeds with title and description, and full_text if available
        feeds = []
        for article in articles[:3]:
            feed = {
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'url': article.get('url', ''),
                'seendate': article.get('seendate', ''),
                'full_text': article.get('full_text', '')
            }
            feeds.append(feed)
        
        # Test send_to_openrouter if feeds available
        if feeds:
            print("Testing send_to_openrouter...")
            gdelt.send_to_openrouter(feeds)
        else:
            print("No feeds to test send_to_openrouter.")
    else:
        print("No articles found.")

if __name__ == "__main__":
    asyncio.run(main())
