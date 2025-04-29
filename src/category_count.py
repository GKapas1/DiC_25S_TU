from mrjob.job import MRJob
import json

"""
Category Count
---
This MapReduce job is to count the number of reviews for each category that appeared in the dataset.
Input: Preprocessed reviews ({"category": ..., "tokens": [...]}) in the previously outputted line-delimited JSON 
Output: (category, category_counts)
---

Funcionality of this script:
- we also need the number of reviews for each of the categories for calculating the chi-square

Key-Value:
- Mapper emits: (category, 1)
- Combiner aggregates locally: (token, partial_sum)
- Reducer sums : (category, count)
"""

#mapreduce job for counting all reviews in each category
class CategoryCount(MRJob):

    def mapper(self, _, line):
        """
        Parse each review line and emit (category, 1) if category exists.
        """
        try:
            review = json.loads(line)
            category = review.get('category', None)

            if category:
                yield category, 1

        except Exception as e:
            self.increment_counter('warn', 'bad_line', 1)

    def reducer(self, category, counts):
        """
        Sum up all review counts per category.
        """
        yield category, sum(counts)

if __name__ == '__main__':
    CategoryCount.run()
