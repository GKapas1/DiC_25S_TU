from mrjob.job import MRJob
import json
from collections import Counter

class TokenCategoryCount(MRJob):

    def mapper(self, _, line):
        try:
            if '\t' in line:
                _, line = line.split('\t', 1)

            data = json.loads(line)

            if isinstance(data, str) and data.startswith('{'):
                data = json.loads(data)

            category = data.get("category")
            tokens = data.get("tokens", [])

            token_cat_pairs = [(token, category) for token in tokens if category]

            token_counter = Counter(token_cat_pairs)

            for token_category, count in token_counter.items():
                yield token_category, count

        except Exception as e:
            self.increment_counter('warn', 'bad_line', 1)

    def combiner(self, token_category, counts):
        yield token_category, sum(counts)

    def reducer(self, token_category, counts):
        yield token_category, sum(counts)

if __name__ == '__main__':
    TokenCategoryCount.run()
