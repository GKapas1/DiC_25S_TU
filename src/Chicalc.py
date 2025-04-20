from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import math

class MRChiSquareCalculator(MRJob):
    #calculating Chi-Square statistic for (term, category) pairs
    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--review-counts', help='Path to review_counts.txt')

    def load_review_counts(self):
        self.total_reviews = 0
        self.category_reviews = {}

        with open(self.options.review_counts, 'r') as f:
            for line in f:
                key_part, value_part = line.strip().split('\t')
                key = json.loads(key_part)
                value = int(value_part)
                if key[0] == "TOTAL":
                    self.total_reviews = value
                elif key[0] == "CATEGORY":
                    self.category_reviews[key[1]] = value

    def steps(self):
        return [
            MRStep(
                mapper_init=self.load_review_counts,
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]

    def mapper(self, _, line):
        key_part, value_part = line.strip().split('\t')
        key = json.loads(key_part)
        A = int(value_part)
        term, category = key

        Nc = self.category_reviews.get(category, 0)
        N = self.total_reviews

        B = Nc - A
        C = sum(
            self.category_reviews.get(cat, 0)
            for cat in self.category_reviews if cat != category
        ) - A
        D = N - A - B - C

        numerator = (A * D - B * C) ** 2
        denominator = (A + B) * (C + D) * (A + C) * (B + D)
        chi2 = (N * numerator / denominator) if denominator != 0 else 0

        yield category, (term, chi2)

    def reducer(self, category, term_scores):
        #Top 75 terms - probably not correct all of them starts with S
        top_terms = sorted(term_scores, key=lambda x: -x[1])[:75]
        yield category, top_terms

if __name__ == '__main__':
    MRChiSquareCalculator.run()
