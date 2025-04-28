from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import math

class MRChiSquareCalculator(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--review-counts', help='Path to review_counts.txt')
        self.add_file_arg('--term-counts', help='Path to token_counts.txt')

    def mapper_init(self):
        # Load category review counts
        self.total_reviews = 0
        self.category_reviews = {}
        self.term_total_counts = {}

        with open(self.options.review_counts, 'r') as f:
            for line in f:
                key_part, value_part = line.strip().split('\t')
                key = json.loads(key_part)
                value = int(value_part)

                if isinstance(key, list) and key[0] == "CATEGORY":
                    category = key[1]
                    self.category_reviews[category] = value
                elif isinstance(key, list) and key[0] == "TOTAL":
                    self.total_reviews = value

        with open(self.options.term_counts, 'r') as f:
            for line in f:
                key_part, value_part = line.strip().split('\t')
                term = json.loads(key_part)
                count = int(value_part)
                self.term_total_counts[term] = count

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        try:
            key_part, value_part = line.strip().split('\t', 1)
            key = json.loads(key_part)
            A = int(value_part)
            term, category = key

            Nc = self.category_reviews.get(category, 0)
            N = self.total_reviews
            B = Nc - A
            C = self.term_total_counts.get(term, 0) - A
            D = N - A - B - C

            numerator = (A * D - B * C) ** 2
            denominator = (A + B) * (C + D) * (A + C) * (B + D)

            chi2 = (N * numerator / denominator) if denominator != 0 else 0

            yield category, (term, chi2)

        except Exception as e:
            self.increment_counter('warn', 'bad_line', 1)

    def reducer(self, category, term_scores):
        top_terms = sorted(term_scores, key=lambda x: -x[1])[:75]

        output_terms = [f"{term}:{chi2:.4f}" for term, chi2 in top_terms]
        yield category, " ".join(output_terms)

if __name__ == '__main__':
    MRChiSquareCalculator.run()