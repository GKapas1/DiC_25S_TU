from transformers import pipeline
import json

# loading pipeline
sentiment_pipeline = pipeline("sentiment-analysis")

def handler(event, context):
    results = []

    for item in event.get("cleaned", []):
        text = item.get("reviewText", "")
        analysis = sentiment_pipeline(text[:512])[0]  # trim to avoid token limit
        item["sentiment"] = analysis["label"]
        item["confidence"] = analysis["score"]
        results.append(item)

    return {"results": results}