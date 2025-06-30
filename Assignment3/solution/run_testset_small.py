import boto3
import json
from uuid import uuid4

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

bucket = "customer-reviews-input"
with open("data/reviews_devset.json") as f:
    reviews = [json.loads(line) for line in f if line.strip()]

for i, review in enumerate(reviews[:5]):  # ⬅️ Only 5 reviews
    key = f"review_{i}_{uuid4().hex[:8]}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(review))
    print(f"Uploaded {key}")

print(f"Uploaded {len(reviews[:5])} reviews to bucket {bucket}.")
