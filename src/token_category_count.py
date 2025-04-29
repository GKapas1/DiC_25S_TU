from mrjob.job import MRJob
import json
from collections import Counter

"""
Token Count per Category
---
This MapReduce job is to count how many times each token appears per category.
Input: Preprocessed reviews ({"category": ..., "tokens": [...]}) in the previously outputted line-delimited JSON 
Output: ((token, category), total_count)
---

Funcionality of this script:
- we need the number of each token per category for calculating the chi-square

Optimizations:
- we use combiner to further reduce intermediate data volume
- we also aggregate locally inside the mapper using collections.Counter

Key-Value:
- Mapper emits: ((token, category), partial_count)
- Combiner aggregates locally
- Reducer sums globally
"""

#mapreduce job for counting tokens per category
class TokenCategoryCount(MRJob):

    def mapper(self, _, line):
        """
        Parse each review line and emit (token, category) pairs with local counts.
        It handles both normal and double-encoded JSON input.
        """
        try:
            if '\t' in line:
                _, line = line.split('\t', 1)

            data = json.loads(line)

            if isinstance(data, str) and data.startswith('{'):
                data = json.loads(data)

            category = data.get("category")
            tokens = data.get("tokens", [])

            #generate all (token, category) pairs
            token_cat_pairs = [(token, category) for token in tokens if category]

            #counting tokens per category locally
            token_counter = Counter(token_cat_pairs)

            for token_category, count in token_counter.items():
                yield token_category, count

        except Exception as e:
            self.increment_counter('warn', 'bad_line', 1)

    def combiner(self, token_category, counts):
        """
        Local partial aggregation at mapper side.
        """
        yield token_category, sum(counts)

    def reducer(self, token_category, counts):
        """
        Global aggregation at reducer side.
        """
        yield token_category, sum(counts)

if __name__ == '__main__':
    TokenCategoryCount.run()
