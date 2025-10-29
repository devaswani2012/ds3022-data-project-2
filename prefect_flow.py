# prefect-flow.py
from prefect import flow, task
import boto3
import requests
import time

# =======================
# Tasks
# =======================

@task
def populate_queue(uva_id: str):
    """Populate SQS queue using the API"""
    url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uva_id}"
    payload = requests.post(url).json()
    print(f"API Response: {payload}")

    sqs_url = payload.get("sqs_url")
    if not sqs_url:
        raise ValueError(f"API did not return 'sqs_url': {payload}")

    print(f"SQS Queue URL: {sqs_url}")
    return sqs_url


@task
def collect_messages(sqs_url: str, expected_count: int = 21):
    """Receive, parse, and delete all messages from SQS"""
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
        in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        delayed = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))

        print(f"\nCollected {len(collected_data)}/{expected_count} messages")
        print(f"Available: {available}, In-flight: {in_flight}, Delayed: {delayed}")

        if available > 0:
            response = sqs.receive_message(
                QueueUrl=sqs_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
                VisibilityTimeout=60,
                MessageAttributeNames=["All"],
            )

            messages = response.get("Messages", [])
            if not messages:
                print("No messages returned despite availability. Waiting 2 seconds...")
                time.sleep(2)
                continue

            for msg in messages:
                msg_id = msg["MessageId"]
                if msg_id in received_ids:
                    continue

                attributes = msg.get("MessageAttributes", {})
                if "order_no" in attributes and "word" in attributes:
                    order_no = attributes["order_no"]["StringValue"]
                    word = attributes["word"]["StringValue"]

                    collected_data[order_no] = word
                    received_ids.add(msg_id)

                    # Delete message immediately
                    sqs.delete_message(
                        QueueUrl=sqs_url,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                    print(f"Received and deleted: order_no={order_no}, word={word}")
                else:
                    print(f"Message missing expected attributes: {msg}")

        else:
            print("No messages available, waiting 5 seconds...")
            time.sleep(5)

    print("\nAll messages collected!")
    return collected_data


@task
def reassemble_phrase(collected_data: dict):
    """Reassemble words into a phrase"""
    ordered_phrase = " ".join(
        collected_data[k] for k in sorted(collected_data, key=lambda x: int(x))
    )
    print("\nReassembled phrase:\n", ordered_phrase)
    return ordered_phrase


@task
def submit_solution(uva_id: str, phrase: str, platform: str = "prefect"):
    """Submit the phrase to the submission SQS queue"""
    sqs = boto3.client("sqs", region_name="us-east-1")
    submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

    try:
        response = sqs.send_message(
            QueueUrl=submission_url,
            MessageBody="Pipeline submission for DP2",
            MessageAttributes={
                "uvaid": {"DataType": "String", "StringValue": uva_id},
                "phrase": {"DataType": "String", "StringValue": phrase},
                "platform": {"DataType": "String", "StringValue": platform},
            },
        )
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 200:
            print("Submission successful! Message ID:", response.get("MessageId"))
            return True
        else:
            print("Submission failed with status code:", status_code)
            return False
    except Exception as e:
        print("Error submitting solution:", e)
        return False


# =======================
# Flow
# =======================

@flow(name="DP2 Prefect Pipeline")
def dp2_flow(uva_id: str):
    sqs_url = populate_queue(uva_id)
    collected_data = collect_messages(sqs_url)
    phrase = reassemble_phrase(collected_data)
    success = submit_solution(uva_id, phrase)

    if not success:
        print("Submission failed. Check logs.")

# =======================
# Run flow
# =======================

if __name__ == "__main__":
    UVA_ID = "vzu3vu"  
    dp2_flow(UVA_ID)

