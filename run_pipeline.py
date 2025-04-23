import os
import subprocess

def run(cmd):
    print(f"▶️ Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

run("python3 src/Ass1_preproc.py")

run("python3 src/create_preprocessed_json.py")

run("python3 src/Counting.py output/preprocessed_reviews.json > output/token_category_counts.txt")

run("python3 src/Totalreview_count.py output/preprocessed_reviews.json > output/review_counts.txt")

run("python3 src/token_counter.py output/preprocessed_reviews.json > output/total_term_counts.txt")

run("python3 src/Chicalc.py output/token_category_counts.txt --review-counts output/review_counts.txt --term-counts total_term_counts.txt> output/chi2_output.txt")

run("python3 src/final_output.py")
