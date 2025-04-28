import json
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default='output/merged_chi2_output.txt', help='Input file path')
    parser.add_argument('--output', default='output/output.txt', help='Output file path')
    args = parser.parse_args()

    category_lines = []
    unique_terms = set()

    with open(args.input, 'r') as f:
        for line in f:
            if not line.strip():
                continue

            parts = line.strip().split('\t')
            if len(parts) != 2:
                continue  # skip malformed lines

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

    # Sort categories alphabetically
    category_lines.sort(key=lambda x: x[0])

    # Sort unique terms alphabetically
    merged_terms_line = ' '.join(sorted(unique_terms))

    with open(args.output, 'w') as f_out:
        for _, line in category_lines:
            f_out.write(line + '\n')
        f_out.write(merged_terms_line + '\n')

    print(f"Formatted output written to {args.output}")

if __name__ == '__main__':
    main()