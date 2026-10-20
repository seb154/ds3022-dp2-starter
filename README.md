# DS 3022 Data Project 2 (Fall 2026): The SQS Puzzle

This data project is a puzzle. Your goal is to put the pieces back together in the right order.

You will write a **Prefect** data pipeline that retrieves 21 messages from an SQS queue, parses their contents, and reassembles a complete phrase from the fragments, one word per message. Your pipeline can run a single time to produce a complete result, or it can run on a schedule. Design your workflow however you see fit. You can run it as many times as you like while testing.

> Adapted from Neal Magee's Fall 2025 Data Project 2 (UVA). The puzzle is the same. Two things are new this year:
> 1. **The queue runs on your laptop.** A free SQS emulator replaces AWS, so you need no AWS account or credentials. Your code is still ordinary `boto3`: one environment variable (`AWS_ENDPOINT_URL_SQS`) decides whether it talks to your laptop or to real AWS.
> 2. **dbt is the quality gate.** Fragments are landed in DuckDB, and a dbt model reassembles the phrase. dbt tests must pass before your flow is allowed to submit.

**Due:** Monday 19 October 2026, **23:59** (Lesson 11 that afternoon is a working session: evidence run, chaos round and independent build time with instructor help). **Weight:** 20%.

Before anything else, follow **[SETUP.md](SETUP.md)** until `python kit/smoke_test.py` prints `Emulator OK`. SETUP.md also explains the four terminals you keep open while you work.
Work through **[CHECKPOINTS.md](CHECKPOINTS.md)**, check your work against **[RUBRIC.md](RUBRIC.md)**, and keep **[PROGRESS.md](PROGRESS.md)** up to date.

## Architecture

```
POST /api/scatter/<UVA_ID>          (kit/scatter_api.py, your laptop)
        |
        v
SQS queue <UVA_ID>  -- 21 messages, each delayed 30-900 s --
        |
        v
Prefect flow  (prefect-flow.py)
   monitor counters -> receive -> land in DuckDB raw.fragments -> delete
        |
        v
dbt build  (dbt/)
   stg_fragments (typed, de-duplicated) -> tests -> assembled_phrase
        |
        v
send_message to queue dp2-submit  ->  python kit/check_submission.py  ->  receipt code
```

## Task 1: Populate your SQS queue

Make an HTTP `POST` request to the scatter API with your UVA computing ID (the scatter API must be running, see SETUP.md):

```
http://127.0.0.1:8000/api/scatter/<UVA_ID>
```

```python
import requests

url = "http://127.0.0.1:8000/api/scatter/mst3k"
payload = requests.post(url).json()
```

```python
import httpx

url = "http://127.0.0.1:8000/api/scatter/mst3k"
payload = httpx.post(url).json()
```

The response gives you your queue URL:

```
>>> payload
{'hello': 'mst3k', 'sqs_url': 'http://127.0.0.1:9324/123456789012/mst3k', 'messages': 21, 'mode': 'fast (5-60s delays)'}
```

Your request sends exactly **21** messages to your queue, each with a random `DelaySeconds` between **30 and 900 seconds** (5 to 60 seconds while `DP2_FAST=1`, for development). **Keep these delays in mind as your pipeline proceeds to the next task.**

**NOTE:** Do not repeat this POST if your pipeline runs more than once (for example on a schedule) or inside a retry loop. Each POST empties your queue and sends all 21 messages again.

## Task 2: Monitor your queue, then collect, land and delete

Track how many messages are waiting with `get_queue_attributes()`. Three values count messages, and together they make up the total:

- `ApproximateNumberOfMessages` (visible, ready to receive)
- `ApproximateNumberOfMessagesNotVisible` (received by someone, not yet deleted)
- `ApproximateNumberOfMessagesDelayed` (still hidden by their delay)

Decide on a strategy for how and when to pick up messages, explain it in your README, and code it.

Each message has a `Body` containing the same meaningless word. The meaningful content is in `MessageAttributes`. Ask for it explicitly with `MessageAttributeNames=["All"]`, then read each attribute's `StringValue`:

```python
msg = resp["Messages"][0]
order_no = msg["MessageAttributes"]["order_no"]["StringValue"]   # a string, not an int
word     = msg["MessageAttributes"]["word"]["StringValue"]
```

Requirements:

- **Land every fragment in DuckDB** (default file `dp2.duckdb`, table `raw.fragments` with at least `order_no`, `word`, `received_at`) **before** you delete its message.
- Delete each message with its `ReceiptHandle` after storing it. Your pipeline must receive, store and delete **all** messages. Leave no "dangling" messages behind.
- Handle empty or delayed responses gracefully. When nothing is visible, `receive_message` returns a response **without** a `Messages` key; your pipeline must not crash on it.
- Use a timeout so the pipeline fails clearly, rather than hanging forever, if fragments never arrive.

## Task 3: Reassemble with dbt, then submit

Build the phrase with dbt inside `dbt/` (the project, profile and source are already set up):

- `models/staging/stg_fragments.sql`: cast `order_no` to an integer and clean `word`. Make this robust to a fragment being stored twice.
- `models/staging/schema.yml`: generic tests (`not_null`, `unique`, ...) on its columns.
- `tests/`: at least one singular test that fails unless there are exactly 21 fragments with no gaps in the order numbers.
- `models/marts/assembled_phrase.sql`: one row containing the phrase in order.

Your flow runs `dbt build --project-dir dbt --profiles-dir dbt` (or `dbtRunner` from `dbt.cli.main`) and **must not submit if dbt fails**.

Example of ordering:

```
order_no   word
--------   -----
3          brown
2          quick
4          fox.
1          The
```

gives "The quick brown fox."

Submit your phrase as a message to the separate queue **`dp2-submit`**, with your computing ID and the platform used (`"prefect"`) as message attributes:

```python
def send_solution(uvaid, phrase, platform):
    url = sqs.get_queue_url(QueueName="dp2-submit")["QueueUrl"]
    response = sqs.send_message(
        QueueUrl=url,
        MessageBody="dp2 solution",
        MessageAttributes={
            "uvaid":    {"DataType": "String", "StringValue": uvaid},
            "phrase":   {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform},
        },
    )
    ...
```

Check and log that the response has HTTP status `200`. Then run:

```
python kit/check_submission.py
```

and paste the **receipt code** it prints into your README.

You may observe and rerun your pipeline as often as you like, but in the end your code must fetch, store, test, reassemble and submit without human intervention. You may not receive messages or sort fragments by hand.

## Notes and submission

1. Fork the assignment repository and push your work to your fork.
2. Your Prefect flow lives in `prefect-flow.py`. Secondary flows (subflows) are allowed.
3. Use Prefect's built-in logging. Do not commit log files.
4. Do not commit data, `.duckdb` files, `dbt/target/` or `.env` (the provided `.gitignore` handles this).
5. Do not edit the files in `kit/`: they are the same for everyone and are used to check your submission.
6. Your **README must include**: how to set up and run your pipeline; your polling strategy and why; how your dbt tests protect the result (with a screenshot of a test failing on purpose and then passing); a Prefect dashboard screenshot of one **full-delay run** (`DP2_FAST=0`); your receipt code; and a "Chaos round" section (Lesson 11).
7. Submit the repository link and your self-assessed **[RUBRIC.md](RUBRIC.md)**. Your README is where you explain and defend your design (25% of the grade).

## Reference

- [Working with SQS: practical examples (Neal Magee)](https://github.com/nmagee/learn-sqs)
- [boto3 SQS client documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html)
- [Prefect documentation](https://docs.prefect.io/v3/get-started)
- [dbt-duckdb adapter](https://github.com/duckdb/dbt-duckdb) and [dbt data tests](https://docs.getdbt.com/docs/build/data-tests)
- [Moto server mode](https://docs.getmoto.org/en/latest/docs/server_mode.html), [ElasticMQ](https://github.com/softwaremill/elasticmq)



# DS 3022 Data Project 2: The SQS Puzzle

**Author:** Seble Bedassa (cjw9zn)
**Receipt code:** `PASS  cjw9zn / prefect  receipt: 79E77759D6`

A Prefect pipeline that POSTs to the scatter API, waits for 21 delayed SQS messages,
lands each fragment in DuckDB, uses dbt to clean, test and reassemble the phrase,
and submits it to the `dp2-submit` queue.

## 1. Setup and run

```bash
git clone <this repo>
cd ds3022-dp2-starter
python3 -m venv .venv
source .venv/bin/activate          1
pip install -r requirements.txt
```
Then open `.env` and set `UVA_ID` to your computing ID.

Open four terminals, activate the venv in each, and start them in this order:

| Terminal | Command | 
|---|---|
| T1 Emulator | `moto_server -p 9324` |
| T2 Scatter API | `python kit/scatter_api.py` | 
| T4 Prefect | `prefect server start` | 
| T3 Work | `python kit/smoke_test.py` (should print `Emulator OK`) |

Then in T3:

```bash
python prefect-flow.py               #runs the whole pipeline
python kit/check_submission.py      #prints PASS and the receipt code
```


## 2. Pipeline design (DAG)

![DAG](docs/dag.jpg)

| Task | What it does | 
|---|---|
| `populate_queue` | Asks the scatter API to put 21 messages in queue |
| `monitor_queue` | Checks the queue every 5 seconds. |
| `collect_messages` | Receives each message, saves it in DuckDB, deletes it from the queue. |
| `dbt_build` | Runs dbt to clean and test the data. Stops the pipeline if a test fails. |
| `read_phrase` | Reads the finished phrase. |
| `submit_solution` | Sends the phrase to `dp2-submit` queue. |


## 3. Polling strategy

**What I did and Why:**
1. Every 5 seconds I check three counts: visible, not visible and delayed. Checking the counts shows me the progress in the logs. 
2. I ask for up to 10 messages at a time. I keep going until I have all 21 words and the queue is empty.  I only start collecting when the messages are actually ready.

**What I didn't do:**
- Wait a fixed 900 seconds. It waits the longest possible time even when the messages are ready sooner.
- Ask for messages nonstop from the start. 

## 4. Receive → store → delete

For each message (in this order):
1. **Receive** it from the queue.
2. **Save** it in DuckDB (table `raw.fragments`).
3. **Delete** it from the queue.

I save before deleting to assure that if the program crashes, the word wouldn't be lost forever. If the program crashes after saving but before deleting, it only makes duplicates which dbt removes later on.

**Empty responses:** when no messages are ready, SQS sends back an answer with no `Messages`. I use `resp.get("Messages", [])`, so I get an empty list instead of a crash.

**Running again:** each run starts with an empty table, no old data.

After collecting, all three queue counts are 0:

![All counters zero](docs/checkB_3counts-are-zero.png)

## 5. dbt tests 

| File | What it does |
|---|---|
| `stg_fragments.sql` | Turns `order_no` into a number, removes extra spaces and duplicate rows. |
| `schema.yml` | Tests that `order_no` is never empty and repeated, and that `word` is never empty. |
| `assert_21_contiguous_fragments.sql` | Fails unless the order numbers are exactly 1 to 21, with no gaps. |
| `assembled_phrase.sql` | Joins the words in order into one phrase. |

**How it protects the result:** if any test fails, dbt doesn't build the phrase it raises an error, and Prefect stops the pipeline. 

**Test failing on purpose (deleted one row):** the 1 to 21 test fails and the phrase is skipped.

![Fail: missing row](docs/checkC_fail_missing_row.png)

**Test failing on purpose (added a second row 5 with a different word):** the "never repeated" test fails.

![Fail: duplicate](docs/checkC_fail_duplicate.png)

**After fixing the data:** all tests pass.

![Pass](docs/checkC_pass.png)

## 6. Chaos round

The queue gets 24 messages instead of 21: 2 duplicates and 1 broken message
that has no word.

**What broke:**
1. Pipeline crashed with `KeyError: 'word'`, because it expected every message to have a word.
2. Loop stopped as soon as it had 21 words, so extra messages could be left in the queue.

![Chaos: broke](docs/checkF_chaos_broke.png)

**How I fixed it:**
- If a message is mising its word or number, I log a warning and delete it without saving it.
- Duplicates are saved, logged, and counted only once.
- The loop now stops only whn I have 21 words and the queue is empty.

**Result:** two chaos runs in a row passed:

![Chaos pass 1](docs/checkF_chaos_pass1.png)
![Chaos pass 2](docs/checkF_chaos_pass2.png)

## 7. Tool roles

| Tool | Job | Without it |
|---|---|---|
| **Prefect** | Runs the steps in order, stops if one fails, and shows logs in a dashboard. | No dashboard or logs. |
| **DuckDB** | Saves each wod as soon as it arrives. | Words could be lost in a crash. |
| **dbt** | Clean and tests the data, then builds phrase. | A missing or repeated word could be submited. |

## 8. Results

**Full-delay run**: completed in 14 min 56 s.

![Full-delay run](docs/checkF_full_delay.png)

**Submission sent** (HTTP status 200):

![Submitted](docs/checkD_submitted_to_dp2-submit.png)

**Check result:** `PASS  cjw9zn / prefect  receipt: 79E77759D6`
