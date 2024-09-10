import os
import sys
import pyodbc
import pandas as pd
import json
import logging
from functools import wraps
from time import sleep
from fpdf import FPDF
from datetime import datetime, timedelta
from reportlab.lib.pagesizes import A4
from reportlab.platypus import Table, TableStyle, Paragraph, Spacer, Image, SimpleDocTemplate, Frame, PageTemplate, BaseDocTemplate, PageBreak
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.units import cm

logging.basicConfig(filename='application_log_vl_full_view.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s:%(message)s')


def retry(operation):
    @wraps(operation)
    def wrapped(*args, **kwargs):
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} failed : {str(e)}")
                sleep(20)  # wait 20 seconds before retrying
        return None

    return wrapped


# Check if we're running as a bundled executable
if getattr(sys, 'frozen', False):
    current_directory = sys._MEIPASS
else:
    current_directory = os.path.abspath(os.path.dirname(__file__))


@retry
def connect_to_database(driver, server_name, database_name, username, password):
    conn_str = (f"DRIVER={driver};SERVER={server_name};"
                f"DATABASE={database_name};UID={username};"
                f"PWD={password};TrustServerCertificate=yes;")
    return pyodbc.connect(conn_str)


@retry
def execute_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    return [dict(zip(column_names, row)) for row in rows]


def get_database_credentials():
    try:
        with open(os.path.join(current_directory, 'credentials_db.json'), 'r') as file:
            all_credentials = json.load(file)
            credentials = all_credentials.get('database')
            if credentials is None:
                logging.error("No database credentials found.")
                return None
            return credentials
    except Exception as e:
        logging.error(f"Failed to read database credentials: {e}")
        return None


def get_query_files():
    return [file for file in os.listdir(current_directory) if file.startswith("query") and file.endswith(".txt")]


def process_queries(conn, queries):
    all_data = []
    for query_file in queries:
        logging.info(f"Processing query file: {query_file}")
        with open(os.path.join(current_directory, query_file), 'r') as file:
            sql_query = file.read()
        data = execute_query(conn, sql_query)
        if data:
            all_data.extend(data)
    return all_data

def mtd_sales_per_dealer(df):
    current_date = datetime.now()
    start_of_month = current_date.replace(day=1)

    # Filter DataFrame for the month-to-date period
    mtd_df = df[(df['delivery_date'] >= start_of_month) & (df['delivery_date'] <= current_date)]
    mtd_df = mtd_df.drop(columns=['delivery_date'])

    return mtd_df


def ytd_sales_per_dealer(df):
    current_year = datetime.now().year

    # Filter DataFrame for the year-to-date period
    ytd_df = df[df['delivery_date'].dt.year == current_year]
    ytd_df = ytd_df.drop(columns=['delivery_date'])
    return ytd_df

def correct_dtypes(df):
    for column in df.columns:
        if column == 'delivery_date':
            try:
                # Convert delivery_date to date and format as DD/MM/YYYY
                df[column] = pd.to_datetime(df[column], format='%d/%m/%Y', errors='coerce')
            except:
                df[column] = df[column].astype(str)  # If conversion fails, keep it as a string
        else:
            try:
                # Try converting to integer
                df[column] = pd.to_numeric(df[column], errors='raise', downcast='integer')
            except:
                try:
                    # Try converting to float
                    df[column] = pd.to_numeric(df[column], errors='raise', downcast='float')
                except:
                    # If all else fails, keep it as a string
                    df[column] = df[column].astype(str)
    return df

def truncate_text(text, max_len):
    if isinstance(text, str) and len(text) > max_len:
        return text[:max_len] + "..."  # Crop and add ellipsis if needed
    return text

def generate_pdf_report(df, report_type):
    pdf_file_name = f"{report_type}_Sales_Report.pdf"
    doc = SimpleDocTemplate(pdf_file_name, pagesize=A4)

    elements = []
    styles = getSampleStyleSheet()

    # Generate the Summary DataFrame
    summary_df = df.groupby('nomen_group_parent').agg({
        'quantity_register_uom': 'sum',
        'total_final_price_czk': 'sum',
        'gross_margin_czk': 'sum'
    }).reset_index()
    # Round and convert numeric columns to integers
    summary_df['total_final_price_czk'] = summary_df['total_final_price_czk'].round(0).astype(int)
    summary_df['gross_margin_czk'] = summary_df['gross_margin_czk'].round(0).astype(int)

    # Calculate Margin %
    summary_df['margin_percent'] = (summary_df['gross_margin_czk'] / summary_df['total_final_price_czk'] * 100).round(2)

    # Prepare the summary table
    summary_table_data = [['Nomen Group Parent', 'Quantity', 'Turnover', 'Margin', 'Margin %']] + \
                         summary_df.values.tolist()
    summary_table = Table(summary_table_data, colWidths=[100, 60, 80, 60, 60])  # Adjusted column widths
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))

    # Prepare Parameters, Details, and Totals
    parameters = [
        "Applied Year: 2024",
        "BP used for Initial Balance: Synthetic",
        "Include Services: No",
        "Invoice Status: Only invoiced deliveries",
        "Currency: CZK",
        "Issue Note Types: External only"
    ]

    details = [
        "- Each transaction is recorded separately",
        "- Gross Margin is defined as selling price - purchase price",
        "  (excluding other purchase costs, credit notes, etc.)",
        "- For initial stock balance, use Synthetic BP",
        "  - Accounting value of the goods is included"
    ]

    total_turnover = df['total_final_price_czk'].sum().round(0).astype(int)
    total_margin = df['gross_margin_czk'].sum().round(0).astype(int)
    avg_margin = (total_margin / total_turnover) * 100 if total_turnover else 0

    totals = [
        f"Turnover: {total_turnover:,.0f} CZK",
        f"Gross Margin: {total_margin:,.0f} CZK",
        f"Avg Gross Margin: {avg_margin:.2f}%"
    ]

    param_table = Table(
        [[Paragraph("<b>Parameters:</b>", styles['Heading3'])]] +  # Title row
        [[Paragraph(item, styles['BodyText'])] for item in parameters],  # Parameter rows
        colWidths=[220]
    )

    details_table = Table(
        [[Paragraph("<b>Details:</b>", styles['Heading3'])]] +  # Title row
        [[Paragraph(item, styles['BodyText'])] for item in details],  # Detail rows
        colWidths=[220]
    )

    totals_table = Table(
        [[Paragraph("<b>Totals:</b>", styles['Heading3'])]] +  # Title row
        [[Paragraph(item, styles['BodyText'])] for item in totals],  # Totals rows
        colWidths=[220]
    )

    # Define layout with a smaller summary table and left column for text
    layout_table = Table(
        [
            [param_table, summary_table],
            [details_table, ''],
            [totals_table, '']
        ],
        colWidths=[240, 260]  # Adjusted column widths for balance
    )

    layout_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('SPAN', (1, 1), (1, 2))  # Span to keep summary table independent
    ]))

    # Add title and layout to the document
    elements.append(Paragraph(f"{report_type} Sales Summary", styles['Title']))
    elements.append(Spacer(1, 12))
    elements.append(layout_table)
    elements.append(PageBreak())

    # Prepare summary per dealer
    dealer_summary_df = df.groupby('dealer_name').agg({
        'quantity_register_uom': 'sum',
        'total_final_price_czk': 'sum',
        'gross_margin_czk': 'sum'
    }).reset_index()
    dealer_summary_df['margin_percent'] = (dealer_summary_df['gross_margin_czk'] / dealer_summary_df['total_final_price_czk'] * 100).round(2)

    # Round the turnover and margin figures for each dealer
    dealer_summary_df['total_final_price_czk'] = dealer_summary_df['total_final_price_czk'].round(0).astype(int)
    dealer_summary_df['gross_margin_czk'] = dealer_summary_df['gross_margin_czk'].round(0).astype(int)

    # Add dealer summary table on second page
    dealer_summary_table_data = [['Dealer', 'Quantity', 'Turnover', 'Margin', 'Margin %']] + \
                                dealer_summary_df.values.tolist()
    dealer_summary_table = Table(dealer_summary_table_data, colWidths=[100, 60, 80, 60, 60])
    dealer_summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))

    elements.append(Paragraph("Dealer Sales Summary", styles['Heading2']))
    elements.append(Spacer(1, 12))
    elements.append(dealer_summary_table)
    elements.append(PageBreak())

    # Add detailed dealer reports
    for dealer_name, dealer_data in df.groupby('dealer_name'):
        elements.append(Spacer(1, 24))
        elements.append(Paragraph(dealer_name, styles['Heading1']))
        elements.append(Spacer(1, 12))

        # Calculate containers as quantity / pcs_container
        dealer_data['container'] = dealer_data.apply(
            lambda row: round(row['quantity_register_uom'] / row['pcs_container'], 2)
            if pd.notna(row['quantity_register_uom']) and pd.notna(row['pcs_container']) and row['pcs_container'] != 0
            else 0,
            axis=1
        )


        # Prepare table data with new "Container" column
        dealer_table_data = [['Nomenclature', 'Brand', 'Total Power (MW)', 'Container', 'Quantity', 'Turnover',
                              'Margin']] + \
                            dealer_data[
                                ['nomenclature', 'brand', 'total_power_mw', 'container', 'quantity_register_uom',
                                 'total_final_price_czk', 'gross_margin_czk']].values.tolist()

        # Truncate text for columns that may overflow
        max_lengths = [20, 20, 10, 10, 10, 15, 10]  # Define max lengths for each column
        dealer_table_data = [[truncate_text(str(item), max_len) for item, max_len in zip(row, max_lengths)]
                             for row in dealer_table_data]

        # Adjust the column widths to fill the full page width (595 points, no margin)
        dealer_table = Table(dealer_table_data, repeatRows=1, colWidths=[100, 100, 100, 65, 65, 85, 80])

        dealer_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.orange),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))

        elements.append(dealer_table)
        elements.append(Spacer(1, 24))

    # Build the PDF document
    doc.build(elements)
def main():
    logging.info(f"Current directory: {current_directory}")

    credentials = get_database_credentials()
    if not credentials:
        return

    queries = get_query_files()
    logging.info(f"Query files found: {queries}")

    conn = connect_to_database(
        credentials['driver'],
        credentials['server'],
        credentials['database'],
        credentials['username'],
        credentials['password']
    )

    if conn:
        all_data = process_queries(conn, queries)
        conn.close()

    df = pd.DataFrame.from_dict(all_data)
    df['dealer_name'] = df['dealer_name'].apply(
        lambda x: str(x).encode('utf-8').decode('utf-8') if isinstance(x, str) else x)
    df = correct_dtypes(df)

    columns_to_keep = [
        'dealer_name', 'nomenclature', 'brand', 'total_power_mw',
        'pcs_container', 'nomen_group_parent', 'quantity_register_uom',
        'register_uom', 'total_final_price_czk', 'gross_margin_czk', 'delivery_date'
    ]

    filtered_df = df[columns_to_keep]

    ytd_df = ytd_sales_per_dealer(filtered_df)  # YTD Data
    mtd_df = mtd_sales_per_dealer(filtered_df)  # MTD Data

    generate_pdf_report(mtd_df, "MTD")  # Generate MTD Report
    generate_pdf_report(ytd_df, "YTD")  # Generate YTD Report


if __name__ == "__main__":
    main()

