from mrjob.job import MRJob
import json

class CategoryCount(MRJob):

    def mapper(self, _, line):
        try:
            review = json.loads(line)
            category = review.get('category', None)

            if category:
                yield category, 1

        except Exception as e:
            self.increment_counter('warn', 'bad_line', 1)

    def reducer(self, category, counts):
        yield category, sum(counts)

if __name__ == '__main__':
    CategoryCount.run()
