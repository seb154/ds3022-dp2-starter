"""DS 3022 Data Project 2: Prefect flow.

Your pipeline must, without human intervention:
  1. POST to the scatter API to populate your queue (once per run, never inside a retry loop)
  2. Monitor the queue (get_queue_attributes) and collect all 21 messages
  3. Land each fragment in DuckDB (raw.fragments) BEFORE deleting its message
  4. Run dbt: tests are the quality gate, the mart model holds the phrase
  5. Submit the phrase to the 'dp2-submit' queue and log the HTTP status code

Run:   python prefect-flow.py
Then:  python kit/check_submission.py

Every task already has a header. Checkpoint A (Lesson 9): fill in populate_queue,
get_counts and monitor_queue. Lesson 10: collect_messages, dbt_build, read_phrase
and submit_solution. The tasks not called by the flow yet cannot fail, so you can
run the file at any point. The skeleton is a suggestion: rename, split or add
tasks as your design needs, and keep your DAG sketch in step with them.
Use Prefect's logger (get_run_logger) for logging. Never hard-code your
computing ID or the endpoint: read them from the environment (.env).
"""
import os
import time
import duckdb
import subprocess

import boto3
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()

UVA_ID = os.environ["UVA_ID"]
SCATTER_URL = f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")

sqs = boto3.client("sqs")  # the endpoint comes from AWS_ENDPOINT_URL_SQS

@task(retries=0)  # never retry: each POST resets the queue and resends all 21 messages
def populate_queue() -> str:
    """POST to the scatter API and return your queue URL."""
    logger = get_run_logger()
    response = requests.post(SCATTER_URL, timeout=30)
    response.raise_for_status()
    payload = response.json()
    queue_url = payload["sqs_url"]  # queue URL returned by the scatter API
    logger.info(f"Queue URL: {queue_url}")
    return queue_url


def get_counts(queue_url: str) -> dict:
    """Plain helper (not a task): return the three ApproximateNumberOf... counters as integers."""
    # Asks SQS how many messages are visible, not visible and delayed
    response = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )
    attrs = response["Attributes"]
    return {
        "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
        "not_visible": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
    }


@task
def monitor_queue(queue_url: str, poll_s: int = 5, timeout_s: int = 1200) -> dict:
    """Log the three counters every poll_s seconds until nothing is delayed; raise after timeout_s."""
    logger = get_run_logger()
    start = time.time()  
    while True:
        counts = get_counts(queue_url)
        logger.info(f"Queue counts: {counts}")
        if counts["delayed"] == 0:
            break
        if time.time() - start > timeout_s:
            raise TimeoutError(f"Timeout exceeded: {timeout_s} seconds")
        time.sleep(poll_s) 
    return counts

@task
def collect_messages(queue_url: str, expected: int = 21, timeout_s: int = 600) -> int:
    """Receive, land in DuckDB, then delete. Return how many fragments are stored."""
    # TODO (Checkpoint B): MessageAttributeNames=["All"] and resp.get("Messages", [])
    logger = get_run_logger()
    start = time.time()

    #"with" opens the DuckDB file and closes it 
    with duckdb.connect(DUCKDB_PATH) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("""
            CREATE OR REPLACE TABLE raw.fragments (
                order_no    VARCHAR,
                word        VARCHAR,
                received_at TIMESTAMP
            )
        """)

        stored = 0
        while True:
            if time.time() - start > timeout_s:
                raise TimeoutError(
                    f"Only {stored} of {expected} fragments arrived within {timeout_s} seconds"
                )

            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,          
                MessageAttributeNames=["All"],   
                WaitTimeSeconds=10,              
            )
            messages = resp.get("Messages", [])  

            for msg in messages:
                attrs = msg.get("MessageAttributes", {})
                order_no = attrs.get("order_no", {}).get("StringValue")
                word = attrs.get("word", {}).get("StringValue")

            
                if order_no is None or word is None or not order_no.isdigit():
                    logger.warning(f"Malformed message deleted (not stored): {attrs}")
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                    continue  

                
                already = con.execute(
                    "SELECT COUNT(*) FROM raw.fragments WHERE order_no = ?", [order_no]
                ).fetchone()[0]
                if already > 0:
                    logger.info(f"Duplicate of fragment {order_no} received (will be counted once)")

                #store it first
                con.execute(
                    "INSERT INTO raw.fragments VALUES (?, ?, current_timestamp)",
                    [order_no, word],
                )
                #then delete it from the queue
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

            
            stored = con.execute(
                "SELECT COUNT(DISTINCT order_no) FROM raw.fragments"
            ).fetchone()[0]
            counts = get_counts(queue_url)
            logger.info(f"Stored {stored}/{expected} fragments; queue counts: {counts}")

            
            if stored >= expected and sum(counts.values()) == 0:
                break

    logger.info(f"Done collecting. Queue counts now: {get_counts(queue_url)}")
    return stored

@task
def dbt_build() -> None:
    """Run `dbt build --project-dir dbt --profiles-dir dbt`; raise if anything fails."""
    # TODO (Checkpoint C)
    logger = get_run_logger()
    result = subprocess.run(
        ["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"],
        capture_output=True,  
        text=True,            
    )
    logger.info(result.stdout)

    if result.returncode != 0:
        raise RuntimeError("dbt build failed (see the log above). The phrase will NOT be submitted.")
    logger.info("dbt build passed: all tests OK")

@task
def read_phrase() -> str:
    """Read the phrase from the dbt mart model in DuckDB."""
    # TODO (Checkpoint C)
    logger = get_run_logger()
    with duckdb.connect(DUCKDB_PATH) as con:
        phrase = con.execute("SELECT phrase FROM assembled_phrase").fetchone()[0]
    logger.info(f"Phrase: {phrase}")
    return phrase

@task
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    """Send to dp2-submit with uvaid / phrase / platform attributes. Return the HTTP status code."""
    # TODO (Checkpoint D)
    logger = get_run_logger()
    submit_url = sqs.get_queue_url(QueueName="dp2-submit")["QueueUrl"]

    response = sqs.send_message(
        QueueUrl=submit_url,
        MessageBody="dp2 solution",  
        MessageAttributes={
            "uvaid":    {"DataType": "String", "StringValue": UVA_ID},
            "phrase":   {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform},
        },
    )

    status = response["ResponseMetadata"]["HTTPStatusCode"]
    logger.info(f"Submitted to dp2-submit: HTTP status {status}")
    if status != 200:
        raise RuntimeError(f"Submission failed with HTTP status {status}")
    return status


@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline():
    queue_url = populate_queue()
    monitor_queue(queue_url)
    collect_messages(queue_url)
    dbt_build()
    phrase = read_phrase()
    submit_solution(phrase)
    # TODO (Lesson 10): collect_messages -> dbt_build -> read_phrase -> submit_solution


if __name__ == "__main__":
    dp2_pipeline()
