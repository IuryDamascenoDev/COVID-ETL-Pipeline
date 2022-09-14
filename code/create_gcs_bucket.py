import os
from google.cloud import storage

os.environ['GOOGLE CREDENTIALS'] = 'your_service_key'

# Instantiating client
storage_client = storage.Client()


# BigQuery's bucket name
bq_bucket_name = "bucket-name"

if __name__ == '__main__':
    # Creating buckets
    bq_bucket = storage_client.create_bucket(bq_bucket_name,
                                             location="region-zone")

    print("Bucket created")
