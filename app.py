from flask import Flask, render_template_string, request, redirect
import os
import glob
import json
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

    # Get txt files
    txt_files = glob.glob(os.path.join(DATA_DIR, "*.txt"))
    txt_files = [f for f in txt_files if not os.path.basename(f).startswith('analysis_')]  # Exclude analysis files
    txt_files.sort(reverse=True)  # Most recent first
    last_txts = []
    for file in txt_files[:5]:  # Last 5
        with open(file, 'r', encoding='utf-8') as f:
            content = f.read()
        title = os.path.basename(file).replace('.txt', '')
        filename = os.path.basename(file)
        last_txts.append({'title': title, 'content': content[:100], 'filename': filename})

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
        <h2>Top OpenRouter Responses</h2>
        {% for analysis in top_analyses %}
        <div class="analysis">
            <strong>{{ analysis.timestamp }}</strong>
            <a href="/analysis/{{ analysis.filename }}" class="link">View Full</a><br>
            {{ analysis.content }}
        </div>
        {% endfor %}
        <h2>Last Few Titles and Data (up to 100 chars)</h2>
        {% for txt in last_txts %}
        <div class="txt">
            <strong>{{ txt.title }}</strong>
            <a href="/txt/{{ txt.filename }}" class="link">View Full</a><br>
            {{ txt.content }}
        </div>
        {% endfor %}

        <h2>Subscribe to Alerts</h2>
        <form action="/subscribe" method="post">
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
    </body>
    </html>
    """
    return render_template_string(html, top_analyses=top_analyses, last_txts=last_txts)

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

@app.route('/subscribe', methods=['POST'])
def subscribe():
    email = request.form.get('email')
    threshold = request.form.get('threshold')
    frequency = request.form.get('frequency')

    if not email or not threshold or not frequency:
        return "Missing required fields", 400

    save_subscriber(email, threshold, frequency)
    return "Subscription successful! You'll receive alerts based on your settings."

if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=7001)
