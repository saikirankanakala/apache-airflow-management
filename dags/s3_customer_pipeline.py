from airflow.sdk import dag, task
from datetime import datetime
import boto3
import csv
import io


BUCKET_NAME = "apache-airflow-management"
INPUT_KEY = "input/customers.csv"
OUTPUT_KEY = "output/processed_customers.csv"


@dag(
    dag_id="s3_customer_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["s3", "etl", "customer"],
)
def s3_customer_pipeline():

    @task
    def extract():
        s3 = boto3.client("s3")

        response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=INPUT_KEY
        )

        content = response["Body"].read().decode("utf-8")

        return content

    @task
    def transform(csv_data):
        reader = csv.DictReader(io.StringIO(csv_data))

        output = io.StringIO()

        fieldnames = [
            "customer_id",
            "name",
            "city",
            "amount",
            "category"
        ]

        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames
        )

        writer.writeheader()

        for row in reader:

            amount = float(row["amount"])

            if amount >= 1500:
                category = "HIGH"
            elif amount >= 1000:
                category = "MEDIUM"
            else:
                category = "LOW"

            writer.writerow({
                "customer_id": row["customer_id"],
                "name": row["name"],
                "city": row["city"],
                "amount": row["amount"],
                "category": category
            })

        return output.getvalue()

    @task
    def load(csv_data):
        s3 = boto3.client("s3")

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=OUTPUT_KEY,
            Body=csv_data.encode("utf-8"),
            ContentType="text/csv"
        )

        return f"s3://{BUCKET_NAME}/{OUTPUT_KEY}"

    data = extract()
    transformed_data = transform(data)
    load(transformed_data)


s3_customer_pipeline()