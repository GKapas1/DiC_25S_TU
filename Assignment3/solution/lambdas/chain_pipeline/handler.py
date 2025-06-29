import boto3
import json
import os

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
lambda_client = boto3.client("lambda")
dynamodb = boto3.resource("dynamodb")

# resource names from SSM Parameter Store
def get_param(name):
    return ssm.get_parameter(Name=name)["Parameter"]["Value"]

def handler(event, context):
    # parse S3 Event
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    # read review JSON from S3
    obj = s3.get_object(Bucket=bucket, Key=key)
    review = json.loads(obj["Body"].read())

    # preprocessing Lambda
    preproc_response = lambda_client.invoke(
        FunctionName=get_param("/lambda/preprocess"),
        Payload=json.dumps({"review": review}),
    )
    preproc_data = json.load(preproc_response["Payload"])

    # profanity-check Lambda
    profanity_response = lambda_client.invoke(
        FunctionName=get_param("/lambda/profanity-check"),
        Payload=json.dumps(preproc_data),
    )
    profanity_data = json.load(profanity_response["Payload"])

    # updating DynamoDB for profanity count
    reviewer = review.get("reviewerID")
    count_table = dynamodb.Table(get_param("/dynamodb/user-table"))
    profane = profanity_data.get("is_profane", False)

    if profane:
        user = count_table.get_item(Key={"reviewerID": reviewer}).get("Item", {"count": 0})
        new_count = user["count"] + 1
        count_table.put_item(Item={"reviewerID": reviewer, "count": new_count})
        if new_count >= 3:
            #banned
            count_table.put_item(Item={"reviewerID": reviewer, "count": new_count, "banned": True})

    # sentiment-analysis Lambda
    sentiment_response = lambda_client.invoke(
        FunctionName=get_param("/lambda/sentiment-analysis"),
        Payload=json.dumps(preproc_data),
    )
    sentiment_data = json.load(sentiment_response["Payload"])

    # store final results to another S3 bucket
    output_bucket = get_param("/s3/output-bucket")
    result_key = f"results/{reviewer}_{key.split('/')[-1]}"
    final_result = {
        "review": review,
        "preprocessed": preproc_data,
        "profanity": profanity_data,
        "sentiment": sentiment_data,
    }
    s3.put_object(
        Bucket=output_bucket,
        Key=result_key,
        Body=json.dumps(final_result).encode("utf-8"),
    )

    return {"status": "processed", "output_key": result_key}
