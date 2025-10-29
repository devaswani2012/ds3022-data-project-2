# prefect flow goes here
from prefect import task, flow, get_run_logger
import requests
import boto3
import time

queue_url = 'https://sqs.us-east-1.amazonaws.com/440848399208/vzu3vu'

@task
def populate_data():
    """
    This task sends a POST request to an API endpoint to 
    to populate the SQS queue with 21 messages
    """

    logger = get_run_logger()

    try:
        url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/vzu3vu"
        response = requests.post(url)
        response.raise_for_status()
        payload = response.json()

        sqs_url = payload.get("sqs_url")
        if sqs_url:
            logger.info(f"Data fetched successfully from {sqs_url}.")
        else:
            logger.warning("No SQS URL found in response payload.")
        return payload

    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP error occurred: {e}")
        return None
    
@task
def get_queue_attributes():
    """
    This task monitors the messages from SQS queue and 
    logs the number of messages available.
    """

    logger = get_run_logger()
    sqs = boto3.client('sqs',region_name='us-east-1')
    elapsed = 0
    logger.info("Waiting for SQS queue to be available...")

    while elapsed < 60:  # Wait for up to 60 seconds
        try:
            response = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages', 
                                'ApproximateNumberOfMessagesNotVisible', 
                                'ApproximateNumberOfMessagesDelayed']
            )
            logger.info("SQS queue is available.")

            attrs = response['Attributes']
            visible = int(attrs.get('ApproximateNumberOfMessages', 0))
            not_visible = int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0))
            delayed = int(attrs.get('ApproximateNumberOfMessagesDelayed', 0))
            total = visible + not_visible + delayed

            logger.info(f"Queue Attributes: Visible: {visible}, Not Visible: {not_visible}, Delayed: {delayed}, Total: {total}")

            if total > 0:
                logger.info("Messages are available in the queue.")
                return attrs
            
            time.sleep(5)
            elapsed += 5
        except Exception as e:
            logger.error(f"An error occurred while checking queue attributes: {e}")
            time.sleep(5)
            elapsed += 5
    logger.warning("SQS queue did not become available within the expected time frame.")
    return None           

@task
def receive_messages():
    """
    This task receives messages from the SQS queue and logs the content.
    """

    logger = get_run_logger()
    sqs = boto3.client('sqs',region_name='us-east-1')
    all_messages = []

    logger.info("Attempting to receive messages from the SQS queue...")
    start_time = time.time()
    MAX_wait = 1000
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5  # short polling
            )
            messages = response.get('Messages', [])

            if not messages:
                elapsed = time.time() - start_time
                if elapsed > MAX_wait:
                    logger.info("No more messages after waiting. Exiting receive loop.")
                    break
                else:
                    logger.info("No messages received yet, waiting...")
                    time.sleep(5)
                    continue

            # Process each message
            for message in messages:
                attrs = message.get('MessageAttributes', {})
                order_no = attrs.get('order_no', {}).get('StringValue', 'N/A')
                word = attrs.get('word', {}).get('StringValue', 'N/A')

                all_messages.append({'order_no': order_no, 'word': word})

                # Delete message after processing
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                logger.info(f"Processed and deleted message: order_no={order_no}, word={word}")

        except Exception as e:
            logger.error(f"Error receiving messages: {e}")
            break

    logger.info(f"Total messages: {len(all_messages)}")
    return all_messages

@task
def send_solution(uvaid, phrase, platform):
    """
    This function sends the final solution to the specified API endpoint.
    """
    logger = get_run_logger()
    sqs = boto3.client('sqs',region_name='us-east-1')
    url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    try:
        response = sqs.send_message(
            QueueUrl=url,
            MessageBody=phrase,
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        logger.info(f"Solution sent successfully: {response.get('MessageId')}")
    except Exception as e:
        logger.error(f"Error sending solution: {e}")
        return None    

@flow
def main_flow():
    uvaid = "vzu3vu"
    platform = "prefect"
    payload = populate_data()
    if payload:
        get_queue_attributes()
        messages = receive_messages()

        messages_sorted = sorted(messages, key=lambda x: int(x['order_no']))
        phrase = " ".join([m['word'] for m in messages_sorted])
        print(f"Final phrase: {phrase}")

        send_solution(uvaid, phrase, platform)
        
if __name__ == "__main__":
    main_flow()
