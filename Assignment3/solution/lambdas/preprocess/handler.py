import json
import string
import boto3
import os
from uuid import uuid4

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",  # for LocalStack
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

def clean_text(text):
    text = text.lower()
    text = text.translate(str.maketrans('', '', string.punctuation))  # remove punctuation
    text = ' '.join(text.split())  # remove whitespace
    return text

def handler(event, context):
    try:
        # Extract bucket and key from the S3 event
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        # Fetch the uploaded review JSON
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read().decode("utf-8")
        review = json.loads(body)

        # Process single review object
        cleaned = {
            "reviewerID": review.get("reviewerID"),
            "reviewerName": review.get("reviewerName"),
            "asin": review.get("asin"),
            "reviewText": clean_text(review.get("reviewText", "")),
            "summary": review.get("summary"),
            "overall": review.get("overall"),
            "category": review.get("category")
        }

        # Get output bucket name from SSM
        ssm = boto3.client(
            "ssm",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1"
        )
        param = ssm.get_parameter(Name="intermediate-bucket")
        next_bucket = param["Parameter"]["Value"]

        # Write cleaned review to output S3
        s3.put_object(
            Bucket=next_bucket,
            Key=f"cleaned/reviews/{uuid4().hex}.json",
            Body=json.dumps(cleaned)
        )

        return {"status": "success", "written_to": next_bucket}

    except Exception as e:
        return {"error": str(e)}
