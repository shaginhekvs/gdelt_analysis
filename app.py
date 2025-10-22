from flask import Flask, render_template_string
import os
import glob
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
        last_txts.append({'title': title, 'content': content[:100]})

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
            <strong>{{ txt.title }}</strong><br>
            {{ txt.content }}
        </div>
        {% endfor %}
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

if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=7001)
