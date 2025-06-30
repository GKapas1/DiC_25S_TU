from better_profanity import profanity
import json
import boto3
import os
from uuid import uuid4

profanity.load_censor_words()

s3 = boto3.client(
    "s3",
    endpoint_url="http://host.docker.internal:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

ssm = boto3.client(
    "ssm",
    endpoint_url="http://host.docker.internal:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

def handler(event, context):
    try:
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]

            # Load the review
            response = s3.get_object(Bucket=bucket, Key=key)
            review = json.loads(response["Body"].read().decode("utf-8"))

            # Check for profanity
            text = review.get("reviewText", "")
            if profanity.contains_profanity(text):
                review["profanity"] = True
            else:
                review["profanity"] = False

            # Output bucket from SSM
            param = ssm.get_parameter(Name="output-bucket")
            output_bucket = param["Parameter"]["Value"]

            # Save flagged review (even if not profane, for full pipeline continuity)
            s3.put_object(
                Bucket=output_bucket,
                Key=f"flagged/{uuid4().hex}.json",
                Body=json.dumps(review)
            )

        return {"status": "done"}

    except Exception as e:
        return {"error": str(e)}
