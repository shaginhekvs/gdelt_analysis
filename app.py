from flask import Flask, render_template_string
import os
import glob
from datetime import datetime

app = Flask(__name__)

DATA_DIR = os.getenv("DATA_DIR")

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
        top_analyses.append({'timestamp': timestamp, 'content': content[:500] + '...' if len(content) > 500 else content})

    # Get txt files
    txt_files = glob.glob(os.path.join(DATA_DIR, "*.txt"))
    txt_files = [f for f in txt_files if not f.endswith('analysis_*.txt')]  # Exclude analysis files
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
        </style>
    </head>
    <body>
        <h1>GDELT Analysis Dashboard</h1>
        <h2>Top OpenRouter Responses</h2>
        {% for analysis in top_analyses %}
        <div class="analysis">
            <strong>{{ analysis.timestamp }}</strong><br>
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

if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=7001)
