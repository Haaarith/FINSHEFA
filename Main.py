from flask import Flask, render_template, request, make_response, session, redirect, session
from flask_session import Session
import redis
import pandas as pd
from io import BytesIO
import json

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Set a secret key for session management

SESSION_TYPE = "filesystem"
# Session configuration
app.config["SESSION_TYPE"] = "redis"
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_USE_SIGNER"] = True
app.config["SESSION_REDIS"] = redis.Redis(host='localhost', port=6379)

# Initialize the session
Session(app)

# Function to calculate additional statistics
def calculate_statistics(azm_df, hyperpay_df, missing_from_azm, missing_from_hyperpay, status_mismatch):
    stats = {}

    # Total number of transactions in each file
    stats['total_azm_transactions'] = azm_df.shape[0]
    stats['total_hyperpay_transactions'] = hyperpay_df.shape[0]

    # Total number of missing transactions
    stats['missing_from_azm_count'] = missing_from_azm.shape[0]
    stats['missing_from_hyperpay_count'] = missing_from_hyperpay.shape[0]

    # Total amount in AZM and HyperPay
    stats['total_amount_azm'] = azm_df['المبلغ (ريال)'].sum()
    stats['total_amount_hyperpay'] = hyperpay_df['Credit'].sum()

    # Total amount of missing transactions from AZM and HyperPay
    stats['total_amount_missing_from_azm'] = missing_from_azm['Credit'].sum() if not missing_from_azm.empty else 0
    stats['total_amount_missing_from_hyperpay'] = missing_from_hyperpay['المبلغ (ريال)'].sum() if not missing_from_hyperpay.empty else 0

    # Total number and amount of mismatched transactions
    stats['status_mismatch_count'] = status_mismatch.shape[0]
    stats['total_amount_status_mismatch'] = status_mismatch['Credit'].sum() if not status_mismatch.empty else 0

    return stats

def compare_transactions(azm_df, hyperpay_df):
    try:
        # Ensure 'Credit' column in hyperpay_df is numeric, converting any non-numeric values to NaN
        hyperpay_df['Credit'] = pd.to_numeric(hyperpay_df['Credit'], errors='coerce')

        # Ensure 'المبلغ (ريال)' column in azm_df is numeric, converting any non-numeric values to NaN
        # Remove commas and convert to numeric
        azm_df['المبلغ (ريال)'] = pd.to_numeric(azm_df['المبلغ (ريال)'].str.replace(',', ''), errors='coerce')

        # Filter HyperPay transactions to include only rows with Result as 'ACK' and Credit > 0
        hyperpay_df = hyperpay_df[hyperpay_df['Credit'] > 0]

        # Clean and standardize merge key columns to avoid mismatches
        azm_df.loc[:, 'تفاصيل العملية (رقم الحوالة)'] = azm_df['تفاصيل العملية (رقم الحوالة)'].str.strip().astype(str)
        hyperpay_df.loc[:, 'TransactionId'] = hyperpay_df['TransactionId'].str.strip().astype(str)

        # Perform the comparison based on the columns provided
        merged_df = pd.merge(
            azm_df[['تاريخ العملية', 'حالة العملية', 'تفاصيل العملية (رقم الحوالة)', 'وسيلة الدفع', 'المبلغ (ريال)']],
            hyperpay_df[['TransactionId', 'Credit', 'RequestTimestamp', 'Result']],
            left_on='تفاصيل العملية (رقم الحوالة)', 
            right_on='TransactionId', 
            how='outer', indicator=True
        )

        # Initialize DataFrames for missing and mismatched transactions
        missing_from_azm = pd.DataFrame()
        missing_from_hyperpay = pd.DataFrame()
        status_mismatch = pd.DataFrame()

        # Iterate through merged DataFrame to classify transactions
        for _, row in merged_df.iterrows():
            if row['_merge'] == 'right_only' and row['Result'] != 'NOK':
                # Missing from AZM
                missing_from_azm = pd.concat([missing_from_azm, row[['TransactionId', 'Credit', 'RequestTimestamp', 'Result']].to_frame().T], ignore_index=True)
            elif row['_merge'] == 'left_only' and row['حالة العملية'] != 'rejected' and row['حالة العملية'] !='time_out' and row['حالة العملية'] != 'failed':
                # Missing from HyperPay
                missing_from_hyperpay = pd.concat([missing_from_hyperpay, row[['تاريخ العملية', 'حالة العملية', 'تفاصيل العملية (رقم الحوالة)', 'وسيلة الدفع', 'المبلغ (ريال)']].to_frame().T], ignore_index=True)
            elif row['_merge'] == 'both' and row['Result'] == 'ACK' and row['حالة العملية'] != 'success':
                # Status mismatch
                status_mismatch = pd.concat([status_mismatch, row.to_frame().T], ignore_index=True)

        # Drop the `_merge` column
        missing_from_azm = missing_from_azm.drop(columns=['_merge'], errors='ignore')
        missing_from_hyperpay = missing_from_hyperpay.drop(columns=['_merge'], errors='ignore')
        status_mismatch = status_mismatch.drop(columns=['_merge'], errors='ignore')

        # Generate HTML tables for missing transactions and status mismatches
        missing_from_azm_table = missing_from_azm.to_html(index=False, classes='table table-striped', border=1) if not missing_from_azm.empty else "No missing transactions from AZM."
        missing_from_hyperpay_table = missing_from_hyperpay.to_html(index=False, classes='table table-striped', border=1) if not missing_from_hyperpay.empty else "No missing transactions from HyperPay."
        status_mismatch_table = status_mismatch.to_html(index=False, classes='table table-striped', border=1) if not status_mismatch.empty else "No status mismatches found."

        # Calculate overall statistics
        stats = calculate_statistics(azm_df, hyperpay_df, missing_from_azm, missing_from_hyperpay, status_mismatch)

        # Return the stats, tables, and mismatches
        return stats, missing_from_azm_table, missing_from_hyperpay_table, status_mismatch_table, missing_from_azm, missing_from_hyperpay, status_mismatch

    except Exception as e:
        # Log the exception for debugging
        print(f"Error during transaction comparison: {e}")
        return {}, "Error occurred during processing.", "Error occurred during processing.", "Error occurred during processing.", pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# Route for the upload page (accessible at /SHEFA)
@app.route('/', methods=['GET', 'POST'])
def upload_files():
    if request.method == 'POST':
        # Get the uploaded files
        azm_files = request.files.getlist('azm_files')  # Multiple files for AZM
        hyperpay_files = request.files.getlist('hyperpay_files')  # Multiple files for HyperPay

        # Merge all AZM files into one DataFrame
        azm_df_list = [pd.read_csv(file) for file in azm_files]
        azm_df = pd.concat(azm_df_list, ignore_index=True)

        # Merge all HyperPay files into one DataFrame
        hyperpay_df_list = [pd.read_csv(file) for file in hyperpay_files]
        hyperpay_df = pd.concat(hyperpay_df_list, ignore_index=True)

        # Compare the transactions
        stats, missing_from_azm_table, missing_from_hyperpay_table, status_mismatch_table, missing_from_azm, missing_from_hyperpay, status_mismatch = compare_transactions(azm_df, hyperpay_df)

        # Save missing transactions and mismatches in session (convert DataFrame to JSON to store it)
        session['missing_from_azm'] = missing_from_azm.to_json(orient='split')
        session['missing_from_hyperpay'] = missing_from_hyperpay.to_json(orient='split')
        session['status_mismatch'] = status_mismatch.to_json(orient='split')

        # Display results with statistics, missing transaction details, and mismatches
        return f'''
        <div class="container">
          <h1 style="color: #4CAF50; text-align: center;">Comparison Results</h1>
          <div class="stats">
            <p><strong>Total AZM Transactions:</strong> {stats['total_azm_transactions']}</p>
            <p><strong>Total HyperPay Transactions:</strong> {stats['total_hyperpay_transactions']}</p>
            <p><strong>Total Missing from AZM:</strong> {stats['missing_from_azm_count']} transactions</p>
            <p><strong>Total Missing from HyperPay:</strong> {stats['missing_from_hyperpay_count']} transactions</p>
            <p><strong>Total Amount in AZM:</strong> {stats['total_amount_azm']} SAR</p>
            <p><strong>Total Amount in HyperPay:</strong> {stats['total_amount_hyperpay']} SAR</p>
            <p><strong>Total Amount Missing from AZM:</strong> {stats['total_amount_missing_from_azm']} SAR</p>
            <p><strong>Total Amount Missing from HyperPay:</strong> {stats['total_amount_missing_from_hyperpay']} SAR</p>
            <p><strong>Total Status Mismatches:</strong> {stats['status_mismatch_count']} transactions</p>
            <p><strong>Total Amount in Status Mismatches:</strong> {stats['total_amount_status_mismatch']} SAR</p>
          </div>
          
          <h2 style="color: #333; text-align: center;">Missing Transactions from AZM</h2>
          <div class="table-container">
            {missing_from_azm_table}
          </div>
          
          <h2 style="color: #333; text-align: center;">Missing Transactions from HyperPay</h2>
          <div class="table-container">
            {missing_from_hyperpay_table}
          </div>

          <h2 style="color: #333; text-align: center;">Status Mismatches</h2>
          <div class="table-container">
            {status_mismatch_table}
          </div>

          <form action="/download" method="post" style="text-align: center;">
            <button type="submit" name="format" value="csv" class="btn">Download as CSV</button>
            <button type="submit" name="format" value="excel" class="btn">Download as Excel</button>
          </form>
        </div>

        <style>
            body {{
            font-family: Arial, sans-serif;
            background-color: #f4f4f4;
            color: #333;
            }}
            .container {{
            max-width: 800px;
            margin: 0 auto;
            background-color: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);
            }}
            h1 {{
            color: #4CAF50;
            margin-bottom: 20px;
            }}
            .stats {{
            margin-bottom: 30px;
            }}
            .table-container {{
            overflow-x: auto;
            margin-bottom: 20px;
            }}
            table {{
            border-collapse: collapse;
            width: 100%;
            text-align: left;
            }}
            th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            }}
            th {{
            background-color: #4CAF50;
            color: white;
            }}
            tr:nth-child(even) {{
            background-color: #f9f9f9;
            }}
            .btn {{
            background-color: #4CAF50;
            color: white;
            padding: 10px 20px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            }}
            .btn:hover {{
            background-color: #45a049;
            }}
        </style>
        '''
    return '''
    <div class="container">
        <h1 style="color: #4CAF50; text-align: center;">Upload CSV Files for AZM and HyperPay</h1>
        <form method="post" enctype="multipart/form-data" style="text-align: center;">
            <label for="azm_files" style="font-weight: bold;">Upload AZM DB CSV Files (You can select multiple):</label><br>
            <input type="file" name="azm_files" multiple accept=".csv" style="margin-bottom: 20px;"><br>
            
            <label for="hyperpay_files" style="font-weight: bold;">Upload HyperPay CSV Files (You can select multiple):</label><br>
            <input type="file" name="hyperpay_files" multiple accept=".csv" style="margin-bottom: 20px;"><br>
            
            <input type="submit" value="Compare Transactions" class="btn">
        </form>
    </div>

    <style>
        body {{
            font-family: Arial, sans-serif;
            background-color: #f4f4f4;
            color: #333;
        }}
        .container {{
            max-width: 600px;
            margin: 0 auto;
            background-color: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);
        }}
        h1 {{
            color: #4CAF50;
            margin-bottom: 20px;
        }}
        .btn {{
            background-color: #4CAF50;
            color: white;
            padding: 10px 20px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
        }}
        .btn:hover {{
            background-color: #45a049;
        }}
    </style>
    '''

# Route to generate and download Excel or CSV (same as before)
@app.route('/download', methods=['POST'])
def download_file():
    format_type = request.form.get('format')

    # Retrieve the missing transactions from session (convert JSON back to DataFrame)
    missing_from_azm = pd.read_json(session.get('missing_from_azm'), orient='split')
    missing_from_hyperpay = pd.read_json(session.get('missing_from_hyperpay'), orient='split')
    status_mismatch = pd.read_json(session.get('status_mismatch'), orient='split')

    if format_type == "csv":
        output = BytesIO()
        missing_from_azm.to_csv(output, index=False)
        output.write(b"\n\n")  # Add space between the two tables in the CSV
        missing_from_hyperpay.to_csv(output, index=False)
        output.seek(0)
        response = make_response(output.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=missing_transactions.csv'
        response.headers['Content-Type'] = 'text/csv'
    else:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            missing_from_azm.to_excel(writer, sheet_name='Missing from AZM', index=False)
            missing_from_hyperpay.to_excel(writer, sheet_name='Missing from HyperPay', index=False)
            status_mismatch.to_excel(writer, sheet_name='Status Mismatch', index=False)
        output.seek(0)
        response = make_response(output.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=missing_transactions.xlsx'
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    return response


@app.route('/IBAN')
def cc():
    return redirect("http://127.0.0.1:5001")

# Run Flask on port 80 for HTTP access (might need sudo for permission)
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
