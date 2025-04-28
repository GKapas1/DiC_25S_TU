from mrjob.job import MRJob
import json

class TokenCategoryCount(MRJob):

    def mapper(self, _, line):
        try:
            # Handle tab-separated key \t value
            if '\t' in line:
                _, line = line.split('\t', 1)

            # First decode JSON
            data = json.loads(line)

            # If still a string (double-encoded), decode again
            if isinstance(data, str):
                data = json.loads(data)

            category = data.get("category", None)
            tokens = data.get("tokens", [])

            if category and tokens:
                for token in tokens:
                    yield (token, category), 1

        except Exception as e:
            self.increment_counter('warn', 'bad_line', 1)

    def reducer(self, token_category, counts):
        yield token_category, sum(counts)

if __name__ == '__main__':
    TokenCategoryCount.run()
