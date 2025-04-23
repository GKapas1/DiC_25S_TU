from mrjob.job import MRJob
import json

class MRTotalTermCounts(MRJob):
    #counting occurencies of tokens in the whole dataset
    def mapper(self, _, line):
        data = json.loads(line)
        category = data["category"]
        tokens = set(data["tokens"])  # use set to count term per review

        for token in tokens:
            yield token, 1

    def reducer(self, term, counts):
        yield term, sum(counts)

if __name__ == '__main__':
    MRTotalTermCounts.run()