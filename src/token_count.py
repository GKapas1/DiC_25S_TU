from mrjob.job import MRJob
import json
from collections import Counter

class MRTotalTermCounts(MRJob):

    def mapper(self, _, line):
        try:
            # Handle Hadoop key-tab-value output
            if '\t' in line:
                _, line = line.split('\t', 1)

            # First json.loads() to get the string (if double-encoded)
            data = json.loads(line)

            if isinstance(data, str):
                data = json.loads(data)

            tokens = data.get('tokens', [])

            # Local aggregation: count tokens inside mapper first
            token_counter = Counter(tokens)

            for token, count in token_counter.items():
                yield token, count

        except Exception as e:
            self.increment_counter('warn', 'bad_line', 1)

    def combiner(self, token, counts):
        yield token, sum(counts)

    def reducer(self, token, counts):
        yield token, sum(counts)

if __name__ == '__main__':
    MRTotalTermCounts.run()
