from mrjob.job import MRJob
import json

class MRTotalTermCounts(MRJob):
    def mapper(self, _, line):
        try:
            # Handle Hadoop key-tab-value output
            if '\t' in line:
                _, line = line.split('\t', 1)

            # First json.loads() to get the string (if double-encoded)
            data = json.loads(line)

            # If it's still a string, load again
            if isinstance(data, str):
                data = json.loads(data)

            tokens = data.get('tokens', [])

            for token in tokens:
                yield token, 1

        except Exception as e:
            self.increment_counter('warn', 'bad_line', 1)

    def reducer(self, token, counts):
        yield token, sum(counts)

if __name__ == '__main__':
    MRTotalTermCounts.run()
