from mrjob.job import MRJob
import json
import re

"""
Preprocessing
---
This is a MapReduce job for preprocessing the large review dataset provided for this exercise.
Input: Line-delimited JSON file about amazon reviews on the dic25_shared directory
Output: Line-delimited JSON for category and tokens of every review: {"category": ..., "tokens": [...] }
---

Funcionality of this script:
- tokenize each review text into lowercase words
- remove stopwords and tokens with length <= 1
- batch results in-memory and flush to output periodically to avoid memory overload - first we tried to output one-by-one
and it was very slow

Optimizations:
- batching 1000 reviews together before emitting (goal: reduce IO overhead)
- loading stopwords only once at the beginning of the mapper
- using MRJob counters to detect missing stopwords or bad lines
"""

#here we define the tokenization delimiters
DELIMITERS = r'[\s\t\d\(\)\[\]\{\}\.!\?,;:+=\-_"]'+r"|\'|`|~|#|@|&|%|\*|\\/|\u20AC|\$|\u00A7"

#mapreduce job for preprocessing
class PreprocessingJob(MRJob):

    def mapper_init(self):
        """
        We load the stopwords once when the mapper starts.
        Also we initialize a batch list to temporarily store processed reviews.
        """
        self.stopwords = set()
        try:
            with open('stopwords.txt', 'r') as f:
                for line in f:
                    self.stopwords.add(line.strip().lower())
        except Exception as e:
            self.stopwords = set()
            self.increment_counter('warn', 'missing_stopwords_file', 1)

        self.batch = []  #to store already processed reviews temporarily
        self.batch_size = 1000  #max number of review per batch - to control memory

    def mapper(self, _, line):
        """
        Process each review = line by line:
        - load JSON line
        - make it lowercase and tokenize review text
        - remove stopwords and short tokens
        - store results in batch
        - when batch is full, emit all and clear
        """
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

            #if batch is full, we yield all items and clear the batch
            if len(self.batch) >= self.batch_size:
                for item in self.batch:
                    yield None, json.dumps(item)
                self.batch.clear()

        except Exception as e:
            self.increment_counter('warn', 'bad_line', 1)

    def mapper_final(self):
        """
        Now we flush the remaining batch when mapper finishes.
        """
        for item in self.batch:
            yield None, json.dumps(item)

if __name__ == '__main__':
    PreprocessingJob.run()