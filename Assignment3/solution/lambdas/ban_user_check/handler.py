import json
import boto3
from uuid import uuid4

s3 = boto3.client(
    "s3",
    endpoint_url="http://host.docker.internal:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url="http://host.docker.internal:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

table = dynamodb.Table("banned-users")

def handler(event, context):
    try:
        for record in event.get("Records", []):
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]

            # Download review
            response = s3.get_object(Bucket=bucket, Key=key)
            review = json.loads(response["Body"].read().decode("utf-8"))

            reviewer_id = review.get("reviewerID")
            is_profane = review.get("profanity", False)

            banned = False
            if is_profane and reviewer_id:
                # Increment offense count
                response = table.update_item(
                    Key={"reviewerID": reviewer_id},
                    UpdateExpression="ADD offense_count :incr",
                    ExpressionAttributeValues={":incr": 1},
                    ReturnValues="UPDATED_NEW"
                )
                count = response["Attributes"]["offense_count"]

                # Ban user if needed
                if count > 3:
                    table.update_item(
                        Key={"reviewerID": reviewer_id},
                        UpdateExpression="SET banned = :true",
                        ExpressionAttributeValues={":true": True}
                    )
                    banned = True

            # If not banned, write review to final output bucket
            if not banned:
                s3.put_object(
                    Bucket="customer-reviews-output",
                    Key=f"final/review_{uuid4().hex}.json",
                    Body=json.dumps(review)
                )
            else:
                s3.put_object(
                    Bucket="customer-reviews-output",
                    Key=f"dropped/banned_{uuid4().hex}.json",
                    Body=json.dumps({
                        "reviewerID": reviewer_id,
                        "reason": "banned",
                        "originalReview": review
                    })
                )

        return {"status": "done"}

    except Exception as e:
        return {"error": str(e)}
