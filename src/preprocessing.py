from mrjob.job import MRJob
import json
import re

# Define your tokenization delimiters
DELIMITERS = r'[\s\t\d\(\)\[\]\{\}\.!\?,;:+=\-_"]'+r"|\'|`|~|#|@|&|%|\*|\\/|\u20AC|\$|\u00A7"

# Load stopwords
class PreprocessingJob(MRJob):

    def mapper_init(self):
        # Load stopwords when mapper starts
        self.stopwords = set()
        try:
            with open('stopwords.txt', 'r') as f:
                for line in f:
                    self.stopwords.add(line.strip().lower())
        except Exception as e:
            self.stopwords = set()
            self.increment_counter('warn', 'missing_stopwords_file', 1)

    def mapper(self, _, line):
        try:
            review = json.loads(line)
            review_text = review.get('reviewText', '')
            category = review.get('category', None)

            if not category:
                return

            # Case folding
            review_text = review_text.lower()

            # Tokenization
            tokens = re.split(DELIMITERS, review_text)

            # Filter tokens
            filtered_tokens = [token for token in tokens if token and token not in self.stopwords and len(token) > 1]

            # Output cleaned review
            output = {
                'category': category,
                'tokens': filtered_tokens
            }
            yield None, json.dumps(output)

        except Exception as e:
            self.increment_counter('warn', 'bad_line', 1)

if __name__ == '__main__':
    PreprocessingJob.run()