from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import boto3
from uuid import uuid4

analyzer = SentimentIntensityAnalyzer()

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
    results = []

    for item in event.get("flagged", []):  # expect input to be from profanity_check
        text = item.get("reviewText", "")
        score = analyzer.polarity_scores(text)

        # Assign sentiment label
        compound = score["compound"]
        if compound >= 0.05:
            sentiment = "POSITIVE"
        elif compound <= -0.05:
            sentiment = "NEGATIVE"
        else:
            sentiment = "NEUTRAL"

        item["sentiment"] = sentiment
        item["confidence"] = abs(compound)
        results.append(item)

    # Fetch output bucket name from SSM
    param = ssm.get_parameter(Name="output-bucket")
    output_bucket = param["Parameter"]["Value"]

    # Store final output to S3
    s3.put_object(
        Bucket=output_bucket,
        Key=f"final/{uuid4().hex}.json",
        Body=json.dumps({"results": results})
    )

    return {"status": "done", "count": len(results)}
