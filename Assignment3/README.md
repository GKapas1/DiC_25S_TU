# Customer Review Processing Pipeline

This project simulates an automated review moderation pipeline using AWS services on LocalStack.

---

## Quick Start

1. Make sure Python 3.11+ is installed.
2. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   .venv\Scripts\activate   # On Windows
   ```

3. Install requirements:

   ```bash
   pip install -r requirements.txt
   ```

4. Run the entire pipeline using:

   ```bash
   app.bat
   ```

   This script will:

   * Activate the virtual environment (if already present)
   * Set up all AWS resources and Lambda functions
   * Upload test reviews to S3
   * Collect and summarize final results

---

## Scripts Overview

* **`setup_resources.py`**:
  Creates S3 buckets, DynamoDB table, SSM parameters, and deploys Lambdas.

* **`run_testset.py`**:
  Uploads 1000 synthetic reviews to the input S3 bucket to trigger the pipeline.

* **`generate_results.py`**:
  Scans final output and reports sentiment stats, profanity matches, and banned users.

---

## What the Pipeline Does

1. A review is uploaded to S3.
2. It triggers preprocessing and profanity filtering.
3. Offending users are tracked and banned if needed.
4. Sentiment is analyzed.
5. Clean reviews reach the final output bucket.

All steps run automatically once the pipeline is initialized.
