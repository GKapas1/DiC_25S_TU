import json
import string
import boto3
import os
from uuid import uuid4

s3 = boto3.client(
    "s3",
    endpoint_url="http://host.docker.internal:4566",  # for LocalStack
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

        # Get cleaned-reviews bucket from SSM
        ssm = boto3.client(
            "ssm",
            endpoint_url="http://host.docker.internal:4566",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1"
        )
        param = ssm.get_parameter(Name="intermediate-bucket")
        cleaned_bucket = param["Parameter"]["Value"]

        key_out = f"cleaned/reviews/{uuid4().hex}.json"
        print(f"Writing to bucket: {cleaned_bucket}, key: {key_out}")

        try:
            s3.put_object(
                Bucket=cleaned_bucket,
                Key=key_out,
                Body=json.dumps(cleaned)
            )
            print("Successfully wrote to S3.")
        except Exception as e:
            print("S3 write failed:", str(e))

        print("Cleaned review:", json.dumps(cleaned))
        return {"status": "success", "written_to": cleaned_bucket}

    except Exception as e:
        return {"error": str(e)}
