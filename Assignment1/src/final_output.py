import json
import argparse

"""
Creating the final output
---
This script formats the final output file after the chi-squared calculation and token selection.
Input: --input argument (default 'output/merged_chi2_output.txt')
Output: --output argument (default 'output/output.txt')
---

Funcionality of this script:
- it makes sure that the format of the output is as the exercise described
- we read the output of the top tokens per category from chi-square calculation
- we format the output properly:
  - one line per category: category_name token1:score token2:score etc...
  - the final line: all unique tokens sorted alphabetically

Steps:
- parse each line: category and its top tokens
- format each category's tokens
- collect all unique tokens across categories
- write sorted category lines + merged sorted token line at the end
"""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default='output/merged_chi2_output.txt', help='Input file path')
    parser.add_argument('--output', default='output/output.txt', help='Output file path')
    args = parser.parse_args()

    category_lines = []
    unique_terms = set()

    #read and process the input file
    with open(args.input, 'r') as f:
        for line in f:
            if not line.strip():
                continue

            parts = line.strip().split('\t')
            if len(parts) != 2:
                continue  #skip malformed lines

            category, terms_str = parts
            terms_list = terms_str.strip().split()

            formatted_terms = []
            for term_score in terms_list:
                if ':' in term_score:
                    term, score = term_score.rsplit(':', 1)
                    formatted_terms.append(f"{term}:{score}")
                    unique_terms.add(term)

            category_line = f"{category} {' '.join(formatted_terms)}"
            category_lines.append((category, category_line))

    #sort categories alphabetically
    category_lines.sort(key=lambda x: x[0])

    #sort unique terms alphabetically
    merged_terms_line = ' '.join(sorted(unique_terms))

    #write the final output file
    with open(args.output, 'w') as f_out:
        for _, line in category_lines:
            f_out.write(line + '\n')
        f_out.write(merged_terms_line + '\n')

    print(f"Formatted output written to {args.output}")

if __name__ == '__main__':
    main()