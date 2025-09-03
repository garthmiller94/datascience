import sys
import subprocess

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    import wheel
except ImportError:
    install("wheel")

try:
    import apache_beam
except ImportError:
    install("apache-beam")
    
import pandas as pd
import sqlite3

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest

def extract_ga(property_id, credentials):
    """Extract data from Google Analytics."""
    client = BetaAnalyticsDataClient(credentials=credentials)
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[{"name": "date"}],
        metrics=[{"name": "activeUsers"}],
        date_ranges=[{"start_date": "2023-01-01", "end_date": "today"}]
    )
    response = client.run_report(request)
    # Convert response to DataFrame
    rows = [
        {dimension.name: row.dimension_values[i].value for i, dimension in enumerate(request.dimensions)}
        | {metric.name: row.metric_values[i].value for i, metric in enumerate(request.metrics)}
        for row in response.rows
    ]
    return pd.DataFrame(rows)

def transform(df):
    df = df.dropna()
    df.columns = [col.lower() for col in df.columns]
    return df

# Usage:
# load_to_bigquery(transformed_data, 'your-gcp-project', 'your_dataset.your_table')

def load_to_bigquery(df, project_id, dataset_table):
    from pandas_gbq import to_gbq
    to_gbq(df, dataset_table, project_id=project_id, if_exists='replace')



if __name__ == "__main__":
    input_file = "input_data.csv"
    db_file = "output_data.db"
    table_name = "etl_table"

    data = extract(input_file)
    transformed_data = transform(data)
    load(transformed_data, db_file, table_name)