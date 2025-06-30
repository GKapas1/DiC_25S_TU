from better_profanity import profanity
import json
import boto3
from uuid import uuid4

profanity.load_censor_words()

# S3 and SSM clients for LocalStack
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

            # Flag if profane
            review["profanity"] = profanity.contains_profanity(review.get("reviewText", ""))

            # Get flagged output bucket from SSM
            param = ssm.get_parameter(Name="flagged-bucket")
            flagged_bucket = param["Parameter"]["Value"]
            key_out = f"flagged/{uuid4().hex}.json"

            print(f"Writing to {flagged_bucket}/{key_out}")
            s3.put_object(
                Bucket=flagged_bucket,
                Key=key_out,
                Body=json.dumps(review)
            )

        return {"status": "done"}

    except Exception as e:
        print("Error:", str(e))
        return {"error": str(e)}
