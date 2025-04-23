import os
import subprocess

def run(cmd):
    print(f"▶️ Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

run("python3 src/preprocessing.py")

run("python3 src/token_category_count.py output/preprocessed_reviews.json > output/token_category_counts.txt")

run("python3 src/category_count.py output/preprocessed_reviews.json > output/category_counts.txt")

run("python3 src/token_count.py output/preprocessed_reviews.json > output/token_counts.txt")

run("python3 src/chisquared_to_sorted_list.py output/token_category_counts.txt --review-counts output/category_counts.txt --term-counts output/token_counts.txt> output/chi2_output.txt")

run("python3 src/final_output.py")
