from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import math

"""
Calculating chi-square and creating a sorted list
---
This MapReduce job computes the chi-squared scores between tokens and categories. It selects the top 75 tokens
per category based on their scores.
Inputs: 
- token_category_counts.txt (input via STDIN) 
- --review-counts review_counts.txt (argument)
- --term-counts token_counts.txt (argument)
Output: (category, "token1:chi2 token2:chi2 ... token75:chi2") for each category
---

Funcionality of this script:
- we also need the number of reviews for each of the categories for calculating the chi-square

Key-Value:
- Mapper emits: (category, (token, chi²_score))
- Reducer emits : (category, top 75 token:score strings)
"""

#mapreduce job for calculating chi-squared score, sorting and extracting the top 75 tokens for each category
class MRChiSquareCalculator(MRJob):

    def configure_args(self):
        """
        This is to add command line arguments to load files:
        - --review-counts : file containing review counts per category
        - --term-counts   : file containing global token counts
        """
        super().configure_args()
        self.add_file_arg('--review-counts', help='Path to review_counts.txt')
        self.add_file_arg('--term-counts', help='Path to token_counts.txt')

    def mapper_init(self):
        """
        Load the data:
        - Total number of reviews
        - Review counts per category
        - Token total occurrence counts
        """
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
        """
        Define single-step MapReduce job: mapper -> reducer
        """
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        """
        Compute Chi-Square score for (token, category) pair using the learned equation.
        Emits (category, (token, chi2 score)).
        """
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
        """
        For each category, sort tokens by descending chi2 and keep only the top 75.
        """
        top_terms = sorted(term_scores, key=lambda x: -x[1])[:75]

        output_terms = [f"{term}:{chi2:.4f}" for term, chi2 in top_terms]
        yield category, " ".join(output_terms)

if __name__ == '__main__':
    MRChiSquareCalculator.run()