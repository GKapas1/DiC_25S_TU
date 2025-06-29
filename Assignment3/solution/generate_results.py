import boto3
import json
import csv

# Setup LocalStack clients with dummy credentials
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

dynamodb = boto3.client(
    "dynamodb",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

# ---- COUNT REVIEW RESULTS FROM OUTPUT BUCKET ----

bucket = "customer-reviews-output"
sentiment_counts = {"POSITIVE": 0, "NEUTRAL": 0, "NEGATIVE": 0}
profanity_count = 0
total_reviews = 0

print("Scanning output reviews from S3...\n")

objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
for obj in objects:
    data = s3.get_object(Bucket=bucket, Key=obj["Key"])
    content = data["Body"].read().decode("utf-8")
    review = json.loads(content)

    # Handle list or single object
    items = review.get("results") or review.get("flagged") or [review]
    for item in items:
        total_reviews += 1
        sentiment = item.get("sentiment")
        if sentiment in sentiment_counts:
            sentiment_counts[sentiment] += 1

        if item.get("profanity"):
            profanity_count += 1

# ---- CHECK BANNED USERS ----

print("Checking banned users from DynamoDB...\n")

banned_users = dynamodb.scan(TableName="banned-users").get("Items", [])
banned_user_ids = [item["reviewerID"]["S"] for item in banned_users]

# ---- OUTPUT RESULTS ----

print("Sentiment Summary:")
for k, v in sentiment_counts.items():
    print(f"  {k.title():<8}: {v}")
print(f"\nTotal Reviews: {total_reviews}")
print(f"\nProfane Reviews: {profanity_count}")
print(f"\nBanned Users ({len(banned_user_ids)}):")
for uid in banned_user_ids:
    print(f"  - {uid}")

csv_path = "results_summary.csv"
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Metric", "Value"])
    writer.writerow(["Total Reviews", total_reviews])
    for sentiment, count in sentiment_counts.items():
        writer.writerow([f"{sentiment.title()} Reviews", count])
    writer.writerow(["Profane Reviews", profanity_count])
    writer.writerow(["Banned Users", len(banned_user_ids)])
    for uid in banned_user_ids:
        writer.writerow([f"Banned User ID", uid])

print(f"Results exported to {csv_path}\n")
