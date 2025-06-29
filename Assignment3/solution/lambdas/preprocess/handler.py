import json
import string

def clean_text(text):
    text = text.lower()
    text = text.translate(str.maketrans('', '', string.punctuation))  # remove punctuation
    text = ' '.join(text.split())  # remove whitespace
    return text

def handler(event, context):
    try:
        if isinstance(event, str):
            event = json.loads(event)

        cleaned_reviews = []

        for line in event:
            cleaned = {
                "reviewerID": line.get("reviewerID"),
                "reviewerName": line.get("reviewerName"),
                "asin": line.get("asin"),
                "cleanedText": clean_text(line.get("reviewText", "")),
                "summary": line.get("summary"),
                "overall": line.get("overall"),
                "category": line.get("category")
            }
            cleaned_reviews.append(cleaned)

        return {"cleaned": cleaned_reviews}

    except Exception as e:
        return {"error": str(e)}
