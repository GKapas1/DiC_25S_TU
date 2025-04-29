from mrjob.job import MRJob
import json
import re

# Define your tokenization delimiters
DELIMITERS = r'[\s\t\d\(\)\[\]\{\}\.!\?,;:+=\-_"]'+r"|\'|`|~|#|@|&|%|\*|\\/|\u20AC|\$|\u00A7"


class PreprocessingJob(MRJob):

    def mapper_init(self):
        self.stopwords = set()
        try:
            with open('stopwords.txt', 'r') as f:
                for line in f:
                    self.stopwords.add(line.strip().lower())
        except Exception as e:
            self.stopwords = set()
            self.increment_counter('warn', 'missing_stopwords_file', 1)

        self.batch = []
        self.batch_size = 1000  # Adjust depending on memory available

    def mapper(self, _, line):
        try:
            review = json.loads(line)
            review_text = review.get('reviewText', '')
            category = review.get('category')

            if not category:
                return

            review_text = review_text.lower()
            tokens = re.split(DELIMITERS, review_text)
            filtered_tokens = [token for token in tokens if token and token not in self.stopwords and len(token) > 1]

            output = {
                'category': category,
                'tokens': filtered_tokens
            }

            self.batch.append(output)

            if len(self.batch) >= self.batch_size:
                for item in self.batch:
                    yield None, json.dumps(item)
                self.batch.clear()

        except Exception as e:
            self.increment_counter('warn', 'bad_line', 1)

    def mapper_final(self):
        for item in self.batch:
            yield None, json.dumps(item)

if __name__ == '__main__':
    PreprocessingJob.run()