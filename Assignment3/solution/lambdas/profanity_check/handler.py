from better_profanity import profanity
import json
import boto3
import os
from uuid import uuid4

profanity.load_censor_words()

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

ssm = boto3.client(
    "ssm",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

def handler(event, context):
    flagged = []

    for item in event.get("cleaned", []):
        text = item.get("reviewText", "")
        if profanity.contains_profanity(text):
            item["profanity"] = True
        flagged.append(item)

    # Get output bucket from SSM
    param = ssm.get_parameter(Name="output-bucket")
    output_bucket = param["Parameter"]["Value"]

    # Write result to output bucket
    s3.put_object(
        Bucket=output_bucket,
        Key=f"flagged/{uuid4().hex}.json",
        Body=json.dumps({"flagged": flagged})
    )

    return {"status": "done", "flagged_count": len(flagged)}
