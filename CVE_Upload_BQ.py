import pandas as pd
from google.cloud import bigquery

# Authenticate to Google Cloud and BigQuery
# ... (follow Google Cloud's authentication instructions)

# Read the Excel file from OneDrive
df = pd.read_excel('path_to_your_excel_file.xlsx')

# Load the DataFrame into BigQuery
client = bigquery.Client()
table_id = "your_project_id.your_dataset_id.your_table_name"
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("column_name", "STRING"),
        # Add more fields as needed
    ]
)