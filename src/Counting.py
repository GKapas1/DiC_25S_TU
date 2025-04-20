from mrjob.job import MRJob
import json

class MRTokenCategoryCounter(MRJob):
    #counting term occurencies per category
    def mapper(self, _, line):
        try:
            review = json.loads(line)
            category = review.get("category")
            tokens = review.get("tokens", [])

            if category:
                for token in tokens:
                    yield (token, category), 1
        except:
            pass  # Skip any malformed lines

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRTokenCategoryCounter.run()