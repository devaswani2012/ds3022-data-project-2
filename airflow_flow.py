# airflow DAG goes here
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import boto3
import time
from airflow.utils.log.logging_mixin import LoggingMixin

api_url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/vzu3vu"
queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/vzu3vu"
submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
uva_id = "vzu3vu"
region = "us-east-1"

log = LoggingMixin().log

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email_on_failure": False,
    "email_on_retry": False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    dag_id='dp2_airflow_flow',
    default_args=default_args,
    description='Populate SQS queue and send solution',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dp2", "sqs", "airflow"],) as dag:


    def populate_data():
        """Send a POST request to populate the SQS queue and return payload."""
        try:
            response = requests.post(api_url)
            response.raise_for_status()
            payload = response.json()
            log.info(f"Data fetched successfully: {payload}")
            return payload 
        except requests.RequestException as e:
            log.error(f"HTTP error occurred: {e}")
            raise

    def get_queue_attributes():
        """Poll SQS until all messages are processed and return attributes."""
        sqs = boto3.client('sqs', region_name=region)
        elapsed = 0
        while elapsed < 900:  # wait up to 15 minutes
            try:
                attrs = sqs.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=['ApproximateNumberOfMessages', 
                                    'ApproximateNumberOfMessagesNotVisible', 
                                    'ApproximateNumberOfMessagesDelayed']
                )['Attributes']

                visible = int(attrs.get('ApproximateNumberOfMessages', 0))
                not_visible = int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0))
                delayed = int(attrs.get('ApproximateNumberOfMessagesDelayed', 0))
                log.info(f"Queue status - Visible: {visible}, Not Visible: {not_visible}, Delayed: {delayed}")

                if visible == 0 and not_visible == 0 and delayed == 0:
                    return attrs

                time.sleep(5)
                elapsed += 5

            except Exception as e:
                log.error(f"Error fetching queue attributes: {e}")
                time.sleep(5)
                elapsed += 5

        log.warning("Timeout reached waiting for SQS queue to empty.")
        return None

    def receive_messages():
        """Receive all messages from the SQS queue and delete them."""
        sqs = boto3.client('sqs', region_name=region)
        messages = []
        start = time.time()
        while True:
            try:
                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    AttributeNames=["All"],
                    MessageAttributeNames=["All"],
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=5,
                )
                msgs = response.get('Messages', [])
                if not msgs:
                    if time.time() - start > 1000:
                        break
                    time.sleep(5)
                    continue

                for msg in msgs:
                    attrs = msg.get('MessageAttributes', {})
                    order_no = attrs.get('order_no', {}).get('StringValue', '0')
                    word = attrs.get('word', {}).get('StringValue', '')
                    messages.append({'order_no': order_no, 'word': word})
                    log.info(f"Received message - order_no: {order_no}, word: {word}")
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg['ReceiptHandle'])

                if not msgs:
                    break

            except Exception as e:
                log.error(f"Error receiving messages: {e}")
                break

        log.info(f"Received total {len(messages)} messages.")
        return messages

    def assemble_and_send_solution(ti):
        """Assemble phrase from messages and send to submit SQS."""
        payload = ti.xcom_pull(task_ids='populate_data_task')
        received_messages = ti.xcom_pull(task_ids='receive_messages_task')

        if not payload or not received_messages:
            raise ValueError("Missing payload or received messages.")

        # Sort messages by order_no
        sorted_msgs = sorted(received_messages, key=lambda x: int(x['order_no']))
        phrase = ' '.join(msg['word'] for msg in sorted_msgs)
        log.info(f"Assembled phrase: {phrase}")

        sqs = boto3.client('sqs', region_name=region)
        try:
            response = sqs.send_message(
                QueueUrl=submit_url,
                MessageBody=phrase,
                MessageAttributes={
                    "uvaid": {"DataType": "String", "StringValue": uva_id},
                    "platform": {"DataType": "String", "StringValue": "airflow"},
                }
            )
            log.info(f"Solution sent successfully with MessageId: {response.get('MessageId')}")
            return response.get('MessageId')
        except Exception as e:
            log.error(f"Error sending solution: {e}")
            raise


    populate_data_task = PythonOperator(
        task_id='populate_data_task',
        python_callable=populate_data,
    )

    get_queue_attributes_task = PythonOperator(
        task_id='get_queue_attributes_task',
        python_callable=get_queue_attributes,
    )

    receive_messages_task = PythonOperator(
        task_id='receive_messages_task',
        python_callable=receive_messages,
    )

    assemble_and_send_solution_task = PythonOperator(
        task_id='assemble_and_send_solution_task',
        python_callable=assemble_and_send_solution,
    )


    populate_data_task >> get_queue_attributes_task >> 
receive_messages_task >> assemble_and_send_solution_task
