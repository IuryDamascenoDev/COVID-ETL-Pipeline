# Visualization pipeline for covid-19 data by the end of 2021 in Brazil

## What this pipeline does  
Takes covid data available at https://covid.saude.gov.br/ and transforms it to fit the visualizations showed in website.

## Dataproc cluster creation  
```bash
gcloud dataproc clusters create [cluster-name] --enable-component-gateway --region [region] --zone [region-zone] --master-machine-type n1-standard-2 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 500 --image-version 1.5-ubuntu18 --project [project-name]
```
## Cloud Storage bucket creation  
```bash
python3 ./code/create_gcs_bucket.py
```
## Upload data to GCS
```bash
gsutil cp -r ./data/ gs://[bucket-name]/
```
## BigQuery dataset creation
```bash
python3 ./code/create_bigquery_dataset.py
```
## Submitting job
```bash
gcloud dataproc jobs submit pyspark ./code/covid_etl.py --cluster=[cluster-name] --region=[region] --jars=gs://spark-lib/bigquery/spark-2.4-bigquery-0.26.0-preview.jar
```
