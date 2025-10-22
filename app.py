from flask import Flask, render_template_string, request, redirect
import os
import glob
import json
import gzip
from datetime import datetime

app = Flask(__name__)

DATA_DIR = os.getenv("DATA_DIR", "data_saved")

@app.route('/')
def index():
    # Get analysis files
    analysis_files = glob.glob(os.path.join(DATA_DIR, "analysis_*.txt"))
    analysis_files.sort(reverse=True)  # Most recent first
    top_analyses = []
    for file in analysis_files[:5]:  # Top 5
        with open(file, 'r', encoding='utf-8') as f:
            content = f.read()
        timestamp = os.path.basename(file).replace('analysis_', '').replace('.txt', '')
        try:
            timestamp = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
        except:
            timestamp = os.path.basename(file)
        filename = os.path.basename(file)
        top_analyses.append({'timestamp': timestamp, 'content': content[:500] + '...' if len(content) > 500 else content, 'filename': filename})

    # Get GDELT cache files
    CACHE_DIR = os.path.join(DATA_DIR, "cache")
    cache_files = glob.glob(os.path.join(CACHE_DIR, "*.gqg.json.gz"))
    cache_files.sort(reverse=True)  # Most recent first
    last_gdelt = []
    for file in cache_files[:5]:  # Last 5
        try:
            with gzip.open(file, 'rt', encoding='utf-8') as f:
                content = f.read()
                # Parse JSON and extract first few articles for preview
                articles = [json.loads(line) for line in content.strip().split('\n') if line.strip()]
                preview = json.dumps(articles[:2], indent=2)  # Show first 2 articles
        except Exception as e:
            preview = f"Error reading file: {e}"

        title = os.path.basename(file).replace('.gqg.json.gz', '')
        filename = os.path.basename(file)
        last_gdelt.append({'title': title, 'content': preview[:200] + '...' if len(preview) > 200 else preview, 'filename': filename})

    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>GDELT Analysis Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1 { color: #333; }
            h2 { color: #555; }
            .analysis { background: #f4f4f4; padding: 10px; margin: 10px 0; border-radius: 5px; }
            .txt { background: #e4f4f4; padding: 10px; margin: 10px 0; border-radius: 5px; }
            .link { color: #007bff; text-decoration: none; }
            .link:hover { text-decoration: underline; }
            pre { white-space: pre-wrap; word-wrap: break-word; }
        </style>
    </head>
    <body>
        <h1>GDELT Analysis Dashboard</h1>

        <h2>Subscribe to Alerts</h2>
        <form action="/subscribe" method="post" style="margin-bottom: 30px;">
            <label for="email">Email:</label><br>
            <input type="email" id="email" name="email" required><br><br>

            <label for="threshold">Minimum Threshold (1-10):</label><br>
            <input type="number" id="threshold" name="threshold" min="1" max="10" value="8" required><br><br>

            <label for="frequency">Frequency (hours):</label><br>
            <select id="frequency" name="frequency">
                <option value="1">Every hour</option>
                <option value="6">Every 6 hours</option>
                <option value="12">Every 12 hours</option>
                <option value="24">Daily</option>
            </select><br><br>

            <input type="submit" value="Subscribe">
        </form>

        <h2>Unsubscribe from Alerts</h2>
        <form action="/unsubscribe" method="post">
            <label for="unsubscribe_email">Email:</label><br>
            <input type="email" id="unsubscribe_email" name="email" required><br><br>
            <input type="submit" value="Unsubscribe">
        </form>

        <h2>Top OpenRouter Responses</h2>
        {% for analysis in top_analyses %}
        <div class="analysis">
            <strong>{{ analysis.timestamp }}</strong>
            <a href="/analysis/{{ analysis.filename }}" class="link">View Full</a><br>
            {{ analysis.content }}
        </div>
        {% endfor %}
        <h2>Latest GDELT Data (Preview)</h2>
        {% for gdelt in last_gdelt %}
        <div class="txt">
            <strong>{{ gdelt.title }}</strong>
            <a href="/gdelt/{{ gdelt.filename }}" class="link">View Full</a><br>
            {{ gdelt.content }}
        </div>
        {% endfor %}
    </body>
    </html>
    """
    return render_template_string(html, top_analyses=top_analyses, last_gdelt=last_gdelt)

@app.route('/analysis/<filename>')
def view_analysis(filename):
    file_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(file_path):
        return "File not found", 404

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    timestamp = filename.replace('analysis_', '').replace('.txt', '')
    try:
        timestamp = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    except:
        timestamp = filename

    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Full Analysis - {{ timestamp }}</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1 { color: #333; }
            .content { background: #f4f4f4; padding: 20px; border-radius: 5px; white-space: pre-wrap; word-wrap: break-word; }
            .back { color: #007bff; text-decoration: none; }
            .back:hover { text-decoration: underline; }
        </style>
    </head>
    <body>
        <h1>Full Analysis</h1>
        <p><strong>Timestamp:</strong> {{ timestamp }}</p>
        <p><a href="/" class="back">← Back to Dashboard</a></p>
        <div class="content">{{ content }}</div>
    </body>
    </html>
    """
    return render_template_string(html, timestamp=timestamp, content=content)

@app.route('/txt/<filename>')
def view_txt(filename):
    file_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(file_path):
        return "File not found", 404

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    title = filename.replace('.txt', '')

    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Full Content - {{ title }}</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1 { color: #333; }
            .content { background: #e4f4f4; padding: 20px; border-radius: 5px; white-space: pre-wrap; word-wrap: break-word; }
            .back { color: #007bff; text-decoration: none; }
            .back:hover { text-decoration: underline; }
        </style>
    </head>
    <body>
        <h1>{{ title }}</h1>
        <p><a href="/" class="back">← Back to Dashboard</a></p>
        <div class="content">{{ content }}</div>
    </body>
    </html>
    """
    return render_template_string(html, title=title, content=content)

@app.route('/gdelt/<filename>')
def view_gdelt(filename):
    CACHE_DIR = os.path.join(DATA_DIR, "cache")
    file_path = os.path.join(CACHE_DIR, filename)
    if not os.path.exists(file_path):
        return "File not found", 404

    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            content = f.read()
            # Parse all articles from the GDELT file
            articles = [json.loads(line) for line in content.strip().split('\n') if line.strip()]
            formatted_content = json.dumps(articles, indent=2)
    except Exception as e:
        formatted_content = f"Error reading file: {e}"

    title = filename.replace('.gqg.json.gz', '')

    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Full GDELT Data - {{ title }}</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1 { color: #333; }
            .content { background: #e4f4f4; padding: 20px; border-radius: 5px; white-space: pre-wrap; word-wrap: break-word; font-family: monospace; }
            .back { color: #007bff; text-decoration: none; }
            .back:hover { text-decoration: underline; }
            .article { border: 1px solid #ccc; margin: 10px 0; padding: 10px; border-radius: 5px; }
        </style>
    </head>
    <body>
        <h1>GDELT Data - {{ title }}</h1>
        <p><strong>Articles:</strong> {{ articles|length }}</p>
        <p><a href="/" class="back">← Back to Dashboard</a></p>
        <div class="content">{{ formatted_content }}</div>
    </body>
    </html>
    """
    return render_template_string(html, title=title, formatted_content=formatted_content, articles=articles)

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

def save_subscriber(email, threshold, frequency):
    """Save new subscriber"""
    sub_file = os.path.join(DATA_DIR, "subscribers.json")
    subscribers = get_subscribers()
    subscribers.append({
        'email': email,
        'threshold': int(threshold),
        'frequency': int(frequency),
        'last_sent': 0
    })
    with open(sub_file, 'w') as f:
        json.dump(subscribers, f, indent=2)

def remove_subscriber(email):
    """Remove subscriber from list"""
    sub_file = os.path.join(DATA_DIR, "subscribers.json")
    subscribers = get_subscribers()
    original_count = len(subscribers)
    subscribers = [sub for sub in subscribers if sub['email'].lower() != email.lower()]
    if len(subscribers) < original_count:
        with open(sub_file, 'w') as f:
            json.dump(subscribers, f, indent=2)
        return True
    return False

@app.route('/subscribe', methods=['POST'])
def subscribe():
    email = request.form.get('email')
    threshold = request.form.get('threshold')
    frequency = request.form.get('frequency')

    if not email or not threshold or not frequency:
        return "Missing required fields", 400

    save_subscriber(email, threshold, frequency)
    return "Subscription successful! You'll receive alerts based on your settings."

@app.route('/unsubscribe', methods=['POST'])
def unsubscribe():
    email = request.form.get('email')

    if not email:
        return "Missing email field", 400

    if remove_subscriber(email):
        return "Successfully unsubscribed from alerts."
    else:
        return "Email not found in subscription list or already unsubscribed."

if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=7001)
