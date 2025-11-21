from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pdfplumber
import pandas as pd
import os
import re
from datetime import datetime
from werkzeug.utils import secure_filename

app = Flask(__name__)
CORS(app)

# Configuration
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'outputs'
ALLOWED_EXTENSIONS = {'pdf'}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def extract_key_value_pairs(text):
    """Extract key-value pairs from PDF text"""
    data = {}
    
    # Common patterns for key-value extraction
    patterns = [
        r'([A-Za-z\s]+?):\s*([^\n]+)',
        r'([A-Za-z\s]+?)\s+([A-Z0-9][^\s]+)',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for key, value in matches:
            key = key.strip()
            value = value.strip()
            if key and value and len(key) < 50:
                data[key] = value
    
    return data

def extract_tables_from_pdf(pdf_path):
    """Extract tables and text from PDF"""
    all_data = []
    text_data = {}
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            # Extract tables
            tables = page.extract_tables()
            for table in tables:
                if table:
                    # Clean and process table
                    df = pd.DataFrame(table[1:], columns=table[0])
                    df = df.dropna(how='all').dropna(axis=1, how='all')
                    all_data.append(df)
            
            # Extract text for key-value pairs
            text = page.extract_text()
            if text:
                page_data = extract_key_value_pairs(text)
                text_data.update(page_data)
    
    return all_data, text_data

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{timestamp}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        try:
            # Extract data from PDF
            tables, text_data = extract_tables_from_pdf(filepath)
            
            # Convert to DataFrame
            if tables:
                df = pd.concat(tables, ignore_index=True)
            else:
                # If no tables, create DataFrame from text data
                df = pd.DataFrame([text_data])
            
            # Save as CSV
            csv_filename = f"{timestamp}_output.csv"
            csv_path = os.path.join(app.config['OUTPUT_FOLDER'], csv_filename)
            df.to_csv(csv_path, index=False)
            
            # Save as XLSX
            xlsx_filename = f"{timestamp}_output.xlsx"
            xlsx_path = os.path.join(app.config['OUTPUT_FOLDER'], xlsx_filename)
            df.to_excel(xlsx_path, index=False)
            
            # Generate insights
            insights = generate_insights(df)
            
            return jsonify({
                'success': True,
                'csv_file': csv_filename,
                'xlsx_file': xlsx_filename,
                'insights': insights,
                'data': df.head(100).to_dict('records'),
                'columns': df.columns.tolist(),
                'total_rows': len(df)
            })
        
        except Exception as e:
            return jsonify({'error': f'Error processing PDF: {str(e)}'}), 500
    
    return jsonify({'error': 'Invalid file type'}), 400

def generate_insights(df):
    """Generate insights from the DataFrame"""
    insights = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'columns': df.columns.tolist(),
        'missing_values': df.isnull().sum().to_dict(),
        'data_types': df.dtypes.astype(str).to_dict(),
        'numeric_stats': {},
        'categorical_summary': {}
    }
    
    # Numeric column statistics
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    for col in numeric_cols:
        insights['numeric_stats'][col] = {
            'mean': float(df[col].mean()) if not df[col].isna().all() else None,
            'median': float(df[col].median()) if not df[col].isna().all() else None,
            'min': float(df[col].min()) if not df[col].isna().all() else None,
            'max': float(df[col].max()) if not df[col].isna().all() else None,
            'std': float(df[col].std()) if not df[col].isna().all() else None
        }
    
    # Categorical column summary
    categorical_cols = df.select_dtypes(include=['object']).columns
    for col in categorical_cols[:5]:  # Limit to first 5 categorical columns
        value_counts = df[col].value_counts().head(10).to_dict()
        insights['categorical_summary'][col] = {
            'unique_values': int(df[col].nunique()),
            'top_values': value_counts
        }
    
    return insights

@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):
    try:
        filepath = os.path.join(app.config['OUTPUT_FOLDER'], filename)
        return send_file(filepath, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 404

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    app.run(debug=True, port=5000)