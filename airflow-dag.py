# airflow DAG goes here

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import boto3
import requests
import time

# =======================
# Tasks
# =======================

@task
def populate_queue(uva_id: str):
    url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uva_id}"
    payload = requests.post(url).json()
    sqs_url = payload.get("sqs_url")
    if not sqs_url:
        raise ValueError(f"API did not return 'sqs_url': {payload}")
    return sqs_url

@task
def collect_messages(sqs_url: str, expected_count: int = 21):
    sqs = boto3.client("sqs", region_name="us-east-1")
    collected_data = {}
    received_ids = set()

    while len(collected_data) < expected_count:
        attrs = sqs.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )["Attributes"]

        available = int(attrs.get("ApproximateNumberOfMessages", 0))
        if available > 0:
            response = sqs.receive_message(
                QueueUrl=sqs_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
                VisibilityTimeout=60,
                MessageAttributeNames=["All"],
            )
            messages = response.get("Messages", [])
            for msg in messages:
                msg_id = msg["MessageId"]
                if msg_id in received_ids:
                    continue
                attrs = msg.get("MessageAttributes", {})
                if "order_no" in attrs and "word" in attrs:
                    order_no = attrs["order_no"]["StringValue"]
                    word = attrs["word"]["StringValue"]
                    collected_data[order_no] = word
                    received_ids.add(msg_id)
                    sqs.delete_message(
                        QueueUrl=sqs_url,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
        else:
            time.sleep(5)
    return collected_data

@task
def reassemble_phrase(collected_data: dict):
    return " ".join(collected_data[k] for k in sorted(collected_data, 
key=lambda x: int(x)))

@task
def submit_solution(uva_id: str, phrase: str, platform: str = "airflow"):
    sqs = boto3.client("sqs", region_name="us-east-1")
    submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    sqs.send_message(
        QueueUrl=submission_url,
        MessageBody="Pipeline submission for DP2",
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": uva_id},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform},
        },
    )
    print("Submitted to Airflow SQS queue!")

# =======================
# DAG definition
# =======================

@dag(
    dag_id="dp2_airflow_pipeline",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["dp2", "airflow"]
)
def dp2_airflow(uva_id: str = "vzu3vu"):
    sqs_url = populate_queue(uva_id)
    collected = collect_messages(sqs_url)
    phrase = reassemble_phrase(collected)
    submit_solution(uva_id, phrase)

dp2_airflow_dag = dp2_airflow()

