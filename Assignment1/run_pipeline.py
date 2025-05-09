import os
import subprocess
import time

"""
Run Pipeline
---
Main script to create the full project pipeline on Hadoop.
---

Funcionality of this script:
- to automatically run all steps of the pipeline in the right order
- to manage HDFS input/output cleanup between stages
- to track and print execution time for each stage (and for the whole project)
"""

def run(cmd, step_name=None):
    """
    Helper function to run a command and time it.
    """
    if step_name:
        print(f"\nStarting: {step_name}")
        start = time.time()
    else:
        start = None

    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

    if start:
        end = time.time()
        print(f"Finished {step_name} in {end - start:.2f} seconds ({(end-start)/60:.2f} minutes)\n")

#user-specific hdfs path
USER='e12433711'

#make sure that the output folder exists
os.makedirs('output', exist_ok=True)

pipeline_start = time.time()


# --- PIPELINE STEPS ---

#1 - preprocessing
try:
    run(f"hdfs dfs -rm -r /user/{USER}/preprocessed_reviews")
except:
    pass
run(f"python3 src/preprocessing.py -r hadoop \
    hdfs:///user/dic25_shared/amazon-reviews/full/reviewscombined.json \
    --output-dir hdfs:///user/{USER}/preprocessed_reviews \
    --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    --files stopwords.txt", step_name="Preprocessing Step")

#2 - token counting per category
try:
    run(f"hdfs dfs -rm -r /user/{USER}/token_category_counts")
except:
    pass
run(f"python3 src/token_category_count.py -r hadoop \
    --jobconf mapreduce.map.memory.mb=4096 \
    --jobconf mapreduce.reduce.memory.mb=4096 \
    --jobconf mapreduce.job.reduces=10 \
    hdfs:///user/{USER}/preprocessed_reviews/part-* \
    --output-dir /user/{USER}/token_category_counts \
    --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar", step_name="Token-Category Count Step")

#3 - category counting
try:
    run(f"hdfs dfs -rm -r /user/{USER}/category_counts")
except:
    pass
run(f"python3 src/category_count.py -r hadoop \
    hdfs:///user/{USER}/preprocessed_reviews/part-* \
    --output-dir /user/{USER}/category_counts \
    --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar", step_name="Category Count Step")

#4 - token counting
try:
    run(f"hdfs dfs -rm -r /user/{USER}/token_counts")
except:
    pass
run(f"python3 src/token_count.py -r hadoop \
    --jobconf mapreduce.job.reduces=10 \
    --jobconf mapreduce.map.memory.mb=4096 \
    --jobconf mapreduce.reduce.memory.mb=4096 \
    hdfs:///user/{USER}/preprocessed_reviews/part-* \
    --output-dir /user/{USER}/token_counts \
    --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar", step_name="Token Count Step")

#5 - chi-squared score calculation
try:
    run(f"hdfs dfs -rm -r /user/{USER}/chi2_output")
except:
    pass
run(f"python3 src/chisquared_to_sorted_list.py -r hadoop \
    hdfs:///user/{USER}/token_category_counts/part-* \
    --review-counts output/category_counts.txt \
    --term-counts output/token_counts.txt \
    --output-dir /user/{USER}/chi2_output \
    --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    --files output/category_counts.txt,output/token_counts.txt", step_name="Chi-Squared Calculation Step")

#6 - merge chi-squared outputs
run(f"hdfs dfs -getmerge /user/{USER}/chi2_output output/merged_chi2_output.txt", step_name="Merging Chi2 Output")

#7 - final formatting
run("python3 src/final_output.py --input output/merged_chi2_output.txt --output output/output.txt", step_name="Final Output Formatting Step")

# --- FINISH ---

pipeline_end = time.time()
print(f"\nFull pipeline completed in {pipeline_end - pipeline_start:.2f} seconds ({(pipeline_end - pipeline_start)/60:.2f} minutes) \n")
