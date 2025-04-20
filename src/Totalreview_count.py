from mrjob.job import MRJob
import json

class MRCategoryReviewCounter(MRJob):
    #helper function that counts total reviews per category and overall
    def mapper(self, _, line):
        try:
            review = json.loads(line)
            category = review.get("category")
            if category:
                yield ("CATEGORY", category), 1
                yield ("TOTAL", "Overall"), 1
        except:
            pass

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRCategoryReviewCounter.run()