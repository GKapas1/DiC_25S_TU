from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import boto3
from uuid import uuid4

analyzer = SentimentIntensityAnalyzer()

# LocalStack S3 and SSM clients
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

            # Load flagged review from S3
            response = s3.get_object(Bucket=bucket, Key=key)
            review = json.loads(response["Body"].read().decode("utf-8"))

            # Sentiment analysis
            text = review.get("reviewText", "")
            score = analyzer.polarity_scores(text)
            compound = score["compound"]

            if compound >= 0.05:
                sentiment = "POSITIVE"
            elif compound <= -0.05:
                sentiment = "NEGATIVE"
            else:
                sentiment = "NEUTRAL"

            review["sentiment"] = sentiment
            review["confidence"] = abs(compound)

            # Get output bucket from SSM
            param = ssm.get_parameter(Name="output-bucket")
            output_bucket = param["Parameter"]["Value"]
            key_out = f"sentiment/{uuid4().hex}.json"

            print(f"Writing to {output_bucket}/{key_out}")
            s3.put_object(
                Bucket=output_bucket,
                Key=key_out,
                Body=json.dumps(review)
            )

        return {"status": "done"}

    except Exception as e:
        print("Error:", str(e))
        return {"error": str(e)}
