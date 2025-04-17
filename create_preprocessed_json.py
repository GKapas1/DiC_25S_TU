import json
import sys
from Ass1_preproc import preprocess

input_file = input_file = 'reviews_devset.json'
output_file = 'preprocessed_reviews.json'

def input_lines():
    if input_file:
        return open(input_file, 'r', encoding='utf-8')
    return sys.stdin

with input_lines() as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
    for line in input_lines():
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
        except Exception as e:
            continue  #skip any bad lines
