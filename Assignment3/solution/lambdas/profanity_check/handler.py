from profanity_check import predict
import json

def handler(event, context):
    flagged = []

    for item in event.get("cleaned", []):
        text = item.get("reviewText", "")
        if predict([text])[0] == 1:
            item["profanity"] = True
            flagged.append(item)

    return {"flagged": flagged}
