import re
import sys
import json

#loading stopwords
def load_stopwords(filepath='stopwords.txt'):
    with open(filepath, 'r', encoding='utf-8') as f:
        stopwords = set(word.strip().lower() for word in f if word.strip())
    return stopwords

STOPWORDS = load_stopwords()

#preprocessing for single text
def preprocess(text:str):
    #tokenizing based on the instructions
    tokens = re.split(r'[\s\d()\[\]{}.!?,;:+=\-_"\'`~#@&*%€$§\\/]+', text)

    #lowercase and filter
    cleaned_tokens = [
        token.lower() for token in tokens
        if token and len(token) > 1 and token.lower() not in STOPWORDS
    ]

    return cleaned_tokens

def preprocess_reviews(input_path, output_path):
    
    with open(input_path, 'r', encoding='utf-8') as f_in, open(output_path, 'w', encoding='utf-8') as f_out:
        for line in f_in:
            try:
                review = json.loads(line)
                text = review.get('reviewText', '')
                category = review.get('category', None)

                if category and text:
                    tokens = preprocess(text)
                    if tokens:
                        output = {
                            "category": category,
                            "tokens": tokens
                        }
                        f_out.write(json.dumps(output) + '\n')
            except Exception:
                continue  # skip malformed lines


if __name__ == '__main__':
    sample_text = """
    This was a gift for my other husband. He's making us things from it all the time!
    Directions are simple, easy to read and interpret.
    """
    print(preprocess(sample_text))

    input_path = 'data/reviews_devset.json'
    output_path = 'output/preprocessed_reviews.json'
    preprocess_reviews(input_path, output_path)
