import json

input_file = "output/chi2_output.txt"
output_file = "output/output.txt"

category_lines = []
unique_terms = set()

with open(input_file, 'r') as f:
    for line in f:
        category, terms_json = line.strip().split('\t')
        terms = json.loads(terms_json)
        
        formatted_terms = []
        for term, score in terms:
            formatted_terms.append(f"{term}:{score:.4f}")
            unique_terms.add(term)

        category_line = f"{category} {' '.join(formatted_terms)}"
        category_lines.append((category, category_line))

#alphabetical category sort
category_lines.sort(key=lambda x: x[0])

#unique term sort alphabetically for the merged dictionary line
merged_terms_line = ' '.join(sorted(unique_terms))

#document everyting to final output.txt
with open(output_file, 'w') as f_out:
    for _, line in category_lines:
        f_out.write(line + '\n')
    f_out.write(merged_terms_line + '\n')

print(f"Formatted output written to {output_file}")
