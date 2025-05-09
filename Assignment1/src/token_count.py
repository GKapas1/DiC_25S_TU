from mrjob.job import MRJob
import json
from collections import Counter

"""
Token Count
---
This MapReduce job is to count how many times each token appears gloablly across all reviews.
Input: Preprocessed reviews ({"category": ..., "tokens": [...]}) in the previously outputted line-delimited JSON 
Output: (token, total_count)
---

Funcionality of this script:
- we need the number of each token in the whole dataset for calculating the chi-square

Optimizations:
- we use combiner to reduce network shuffle size
- we also aggregate the token counts locally inside the mapper using collections.Counter

Key-Value:
- Mapper emits: (token, partial_count)
- Combiner aggregates locally: (token, partial_sum)
- Reducer sums all partial sums: (token, global_sum)
"""

#mapreduce job for counting tokens in all reviews
class MRTotalTermCounts(MRJob):

    def mapper(self, _, line):
        """
        Parse each line, count tokens locally using Counter and emit (token, local_count).
        """
        try:
            #to handle Hadoop key-tab-value output
            if '\t' in line:
                _, line = line.split('\t', 1)

            #json.loads() to get the string (if double-encoded)
            data = json.loads(line)

            if isinstance(data, str):
                data = json.loads(data)

            tokens = data.get('tokens', [])

            #local aggregation: count tokens inside mapper first
            token_counter = Counter(tokens)

            for token, count in token_counter.items():
                yield token, count

        except Exception as e:
            self.increment_counter('warn', 'bad_line', 1)

    def combiner(self, token, counts):
        """
        Local pre-aggregation before sending to reducer.
        """
        yield token, sum(counts)

    def reducer(self, token, counts):
        """
        Final aggregation across all mappers.
        """
        yield token, sum(counts)

if __name__ == '__main__':
    MRTotalTermCounts.run()
