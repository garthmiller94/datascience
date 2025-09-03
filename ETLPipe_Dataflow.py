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

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

def extract(pipeline, input_file):
    # Read from CSV (simulate external warehouse extract)
    return (
        pipeline
        | 'ReadCSV' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
        | 'ParseCSV' >> beam.Map(lambda line: dict(zip(
            ['col1', 'col2', 'col3'], line.split(',')
        )))
    )

def transform(records):
    # Example transformation: drop records with missing values, lowercase keys
    def clean_record(record):
        if all(record.values()):
            return {k.lower(): v for k, v in record.items()}
        return None
    return (
        records
        | 'CleanRecords' >> beam.Map(clean_record)
        | 'FilterNone' >> beam.Filter(lambda x: x is not None)
    )

def load(records, project_id, dataset_table):
    # Write to BigQuery
    return (
        records
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            dataset_table,
            project=project_id,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

if __name__ == "__main__":
    input_file = "input_data.csv"
    project_id = "your-gcp-project"
    dataset_table = "your_dataset.your_table"

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.job_name = "etl-dataflow-job"
    google_cloud_options.staging_location = "gs://your-bucket/staging"
    google_cloud_options.temp_location = "gs://your-bucket/temp"
    options.view_as(StandardOptions).runner = "DataflowRunner"

    with beam.Pipeline(options=options) as p:
        records = extract(p, input_file)
        transformed = transform(records)
        load(transformed, project_id, dataset_table)

# Note: Replace "your-gcp-project", "your_dataset.your_table", and GCS paths with actual values.
"""
Notes:

Replace column names in ParseCSV with your actual schema.
Update project_id, dataset_table, staging_location, and temp_location with your GCP details.
This pipeline reads from a CSV, transforms, and loads to BigQuery using Dataflow.
"""