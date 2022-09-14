from google.cloud import bigquery
import os

os.environ['GOOGLE CREDENTIALS'] = 'your_service_key'

client = bigquery.Client()

project = 'project-name'
dataset_id = f"{project}.covid_views"

dataset = bigquery.Dataset(dataset_id)

dataset.location = "us-central1"

dataset = client.create_dataset(dataset, timeout=30)
print("Created dataset")
