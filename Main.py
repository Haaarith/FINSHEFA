from flask import Flask, render_template, request, make_response, session
import pandas as pd
from io import BytesIO
import json

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Set a secret key for session management

# Function to calculate additional statistics
def calculate_statistics(azm_df, hyperpay_df, missing_from_azm, missing_from_hyperpay):
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

    return stats

def compare_transactions(azm_df, hyperpay_df):
    # Filter HyperPay transactions where Credit is greater than 0
    hyperpay_df = hyperpay_df[hyperpay_df['Credit'] > 0]

    # Perform the comparison based on the columns provided
    merged_df = pd.merge(
        azm_df[['تاريخ العملية', 'حالة العملية', 'تفاصيل العملية (رقم الحوالة)', 'وسيلة الدفع', 'المبلغ (ريال)']],
        hyperpay_df[['TransactionId', 'Credit', 'RequestTimestamp']],
        left_on='تفاصيل العملية (رقم الحوالة)', 
        right_on='TransactionId', 
        how='outer', indicator=True
    )

    # Get statistics of missing transactions
    missing_from_azm = merged_df[merged_df['_merge'] == 'right_only'].dropna(axis=1, how='all')
    missing_from_hyperpay = merged_df[merged_df['_merge'] == 'left_only'].dropna(axis=1, how='all')

    # Drop the `_merge` column
    missing_from_azm = missing_from_azm.drop(columns=['_merge'])
    missing_from_hyperpay = missing_from_hyperpay.drop(columns=['_merge'])

    # Generate HTML tables for missing transactions
    missing_from_azm_table = missing_from_azm.to_html(index=False, classes='table table-striped', border=0)
    missing_from_hyperpay_table = missing_from_hyperpay.to_html(index=False, classes='table table-striped', border=0)

    # Calculate overall statistics
    stats = calculate_statistics(azm_df, hyperpay_df, missing_from_azm, missing_from_hyperpay)

    # Return the three values: stats, missing_from_azm_table, missing_from_hyperpay_table, missing_from_azm, missing_from_hyperpay
    return stats, missing_from_azm_table, missing_from_hyperpay_table, missing_from_azm, missing_from_hyperpay

# Route for the upload page
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
        stats, missing_from_azm_table, missing_from_hyperpay_table, missing_from_azm, missing_from_hyperpay = compare_transactions(azm_df, hyperpay_df)

        # Save missing transactions in session (convert DataFrame to JSON to store it)
        session['missing_from_azm'] = missing_from_azm.to_json(orient='split')
        session['missing_from_hyperpay'] = missing_from_hyperpay.to_json(orient='split')

        # Display results with statistics and missing transaction details
        return f'''
        <h3>Comparison Results</h3>
        <p><strong>Total AZM Transactions:</strong> {stats['total_azm_transactions']}</p>
        <p><strong>Total HyperPay Transactions:</strong> {stats['total_hyperpay_transactions']}</p>
        <p><strong>Total Missing from AZM:</strong> {stats['missing_from_azm_count']} transactions</p>
        <p><strong>Total Missing from HyperPay:</strong> {stats['missing_from_hyperpay_count']} transactions</p>
        <p><strong>Total Amount in AZM:</strong> {stats['total_amount_azm']} SAR</p>
        <p><strong>Total Amount in HyperPay:</strong> {stats['total_amount_hyperpay']} SAR</p>
        <p><strong>Total Amount Missing from AZM:</strong> {stats['total_amount_missing_from_azm']} SAR</p>
        <p><strong>Total Amount Missing from HyperPay:</strong> {stats['total_amount_missing_from_hyperpay']} SAR</p>
        
        <h3>Missing Transactions from AZM</h3>
        {missing_from_azm_table}

        <h3>Missing Transactions from HyperPay</h3>
        {missing_from_hyperpay_table}

        <form action="/download" method="post">
          <button type="submit" name="format" value="csv">Download as CSV</button>
          <button type="submit" name="format" value="excel">Download as Excel</button>
        </form>

        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
            }}
            h3 {{
                color: #333;
            }}
            table {{
                width: 100%;
                margin-bottom: 20px;
                border-collapse: collapse;
            }}
            table, th, td {{
                border: 1px solid #ddd;
            }}
            th, td {{
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            form {{
                margin-top: 20px;
            }}
        </style>
        '''

    return '''
    <h1>Upload CSV Files for AZM and HyperPay</h1>
    <form method="post" enctype="multipart/form-data">
      <label for="azm_files">Upload AZM DB CSV Files (You can select multiple):</label><br>
      <input type="file" name="azm_files" multiple accept=".csv"><br><br>
      <label for="hyperpay_files">Upload HyperPay CSV Files (You can select multiple):</label><br>
      <input type="file" name="hyperpay_files" multiple accept=".csv"><br><br>
      <input type="submit" value="Compare Transactions" class="btn btn-primary">
    </form>

    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
        }}
        label {{
            font-weight: bold;
        }}
        .btn {{
            background-color: #4CAF50;
            color: white;
            padding: 10px 20px;
            border: none;
            cursor: pointer;
            border-radius: 5px;
        }}
        .btn:hover {{
            background-color: #45a049;
        }}
    </style>
    '''

# Route to generate and download Excel or CSV
@app.route('/download', methods=['POST'])
def download_file():
    format_type = request.form.get('format')

    # Retrieve the missing transactions from session (convert JSON back to DataFrame)
    missing_from_azm = pd.read_json(session.get('missing_from_azm'), orient='split')
    missing_from_hyperpay = pd.read_json(session.get('missing_from_hyperpay'), orient='split')

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
        output.seek(0)
        response = make_response(output.getvalue())
        response.headers['Content-Disposition'] = 'attachment; filename=missing_transactions.xlsx'
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    return response


if __name__ == '__main__':
    app.run(debug=True)
