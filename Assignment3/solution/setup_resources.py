import subprocess
import os
import json
import shutil
from pathlib import Path

LAMBDA_FUNCTIONS = [
    "preprocess",
    "profanity_check",
    "sentiment_analysis",
    "ban_user_check"
]

def run(cmd):
    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

def setup_buckets():
    run("awslocal s3 mb s3://customer-reviews-input")
    run("awslocal s3 mb s3://cleaned-reviews")
    run("awslocal s3 mb s3://customer-reviews-output")

def setup_dynamodb():
    result = subprocess.run(
        "awslocal dynamodb list-tables",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )
    existing_tables = json.loads(result.stdout.decode()).get("TableNames", [])

    if "banned-users" in existing_tables:
        print("Table 'banned-users' already exists. Skipping creation.")
        return

    run("""awslocal dynamodb create-table \
        --table-name banned-users \
        --attribute-definitions AttributeName=reviewerID,AttributeType=S \
        --key-schema AttributeName=reviewerID,KeyType=HASH \
        --billing-mode PAY_PER_REQUEST""")

def setup_ssm():
    existing_params = subprocess.run(
        "awslocal ssm describe-parameters",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )
    param_names = [param["Name"] for param in json.loads(existing_params.stdout.decode()).get("Parameters", [])]

    def put_if_missing(name, value):
        if name in param_names:
            print(f"SSM parameter '{name}' already exists. Skipping.")
        else:
            run(f"awslocal ssm put-parameter --name {name} --type String --value {value} --overwrite")

    put_if_missing("input-bucket", "customer-reviews-input")
    put_if_missing("intermediate-bucket", "cleaned-reviews")
    put_if_missing("output-bucket", "customer-reviews-output")
    put_if_missing("banned-users-table", "banned-users")

def package_and_deploy_lambdas():
    for name in LAMBDA_FUNCTIONS:
        path = Path(f"lambdas/{name}")
        zip_path = path / "lambda.zip"
        package_path = path / "package"

        # Clean up previous build
        shutil.rmtree(package_path, ignore_errors=True)
        if zip_path.exists():
            zip_path.unlink()

        # Recreate package directory
        package_path.mkdir(parents=True, exist_ok=True)

        # Install dependencies
        run(
            f"pip install -r {path}/requirements.txt -t {package_path} "
            f"--platform manylinux2014_x86_64 --only-binary=:all: --implementation cp --python-version 3.11 --abi cp311"
        )

        # Copy lambda handler
        shutil.copy(path / "handler.py", package_path)

        # Zip the package
        shutil.make_archive(base_name=zip_path.with_suffix(""), format="zip", root_dir=package_path)

        # Deploy to LocalStack
        try:
            run(
                f"""awslocal lambda create-function \
                    --function-name {name} \
                    --runtime python3.11 \
                    --handler handler.handler \
                    --role arn:aws:iam::000000000000:role/execution_role \
                    --zip-file fileb://{zip_path}"""
            )
        except subprocess.CalledProcessError as e:
            print(f"Function {name} already exists, updating...")
            run(f"""awslocal lambda update-function-code \
                --function-name {name} \
                --zip-file fileb://{zip_path}""")


def setup_s3_trigger():
    #S3 trigger to preprocessing Lambda
    notification_json = '''{
    "LambdaFunctionConfigurations": [{
        "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:preprocess",
        "Events": ["s3:ObjectCreated:*"]
    }]
    }'''

    with open("notification.json", "w") as f:
        f.write(notification_json)
    run("awslocal s3api put-bucket-notification-configuration --bucket customer-reviews-input --notification-configuration file://notification.json")
    os.remove("notification.json")

def setup_profanity_trigger():
    notification_json = '''{
      "LambdaFunctionConfigurations": [{
        "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:profanity_check",
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
          "Key": {
            "FilterRules": [{
              "Name": "prefix",
              "Value": "cleaned/reviews/"
            }]
          }
        }
      }]
    }'''

    with open("profanity_notification.json", "w") as f:
        f.write(notification_json)

    run("awslocal s3api put-bucket-notification-configuration "
        "--bucket cleaned-reviews "
        "--notification-configuration file://profanity_notification.json")

    os.remove("profanity_notification.json")


def setup_sentiment_trigger():
    notification_json = '''{
      "LambdaFunctionConfigurations": [{
        "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:sentiment_analysis",
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
          "Key": {
            "FilterRules": [{
              "Name": "prefix",
              "Value": "flagged/"
            }]
          }
        }
      }]
    }'''

    with open("sentiment_notification.json", "w") as f:
        f.write(notification_json)

    run("awslocal s3api put-bucket-notification-configuration "
        "--bucket customer-reviews-output "
        "--notification-configuration file://sentiment_notification.json")

    os.remove("sentiment_notification.json")

def setup_ban_user_trigger():
    notification_json = '''{
      "LambdaFunctionConfigurations": [{
        "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:ban_user_check",
        "Events": ["s3:ObjectCreated:*"],
        "Filter": {
          "Key": {
            "FilterRules": [{
              "Name": "prefix",
              "Value": "flagged/"
            }]
          }
        }
      }]
    }'''

    with open("ban_notification.json", "w") as f:
        f.write(notification_json)

    run("awslocal s3api put-bucket-notification-configuration "
        "--bucket customer-reviews-output "
        "--notification-configuration file://ban_notification.json")

    os.remove("ban_notification.json")


def main():
    setup_buckets()
    setup_dynamodb()
    setup_ssm()
    package_and_deploy_lambdas()
    setup_s3_trigger()
    setup_profanity_trigger()
    setup_sentiment_trigger()
    setup_ban_user_trigger()

if __name__ == "__main__":
    main()
