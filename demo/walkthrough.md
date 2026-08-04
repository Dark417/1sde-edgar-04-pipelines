# Walkthrough — run it, then check what it did

Two systems run on a schedule, and each keeps its history in a different place:

| | where it runs | where its history lives |
|---|---|---|
| **Collection** (repo 3) | AWS ECS Fargate | CloudWatch Logs |
| **Processing** (repo 4) | Databricks job | Databricks run history |

Nothing below needs a bucket name or account number typed in — every command resolves
those at runtime from Parameter Store, which is where they are published.

---

## Before anything: a Windows gotcha that will waste an hour

On Git Bash, arguments that look like Unix paths are silently rewritten into Windows
paths. `/Volumes/edgar/...` becomes `C:/Program Files/Git/Volumes/edgar/...` before the
command ever sees it, and the error you get back names a path you never typed.

Export this once per shell:

```bash
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
```

This is not theoretical — it is what produced `No FileSystem for scheme "C"` from a Spark
job that had nothing wrong with it.

---

## 1. Collection — what ran, and what it fetched

**Did the schedule fire, and is it on?**

```bash
aws scheduler get-schedule --name edgar-lakehouse-ingest-daily \
  --query '[State,ScheduleExpression]' --output text
# ENABLED   cron(0 6 * * ? *)
```

**Recent runs.** Each Fargate task writes one log stream, newest first:

```bash
aws logs describe-log-streams \
  --log-group-name /ecs/edgar-lakehouse-ingest \
  --order-by LastEventTime --descending \
  --query 'logStreams[:10].[logStreamName,lastEventTimestamp]' --output table
```

**What a run actually did.** The logs are structured JSON, one object per event, so they
can be read or queried rather than skimmed:

```bash
STREAM=$(aws logs describe-log-streams \
  --log-group-name /ecs/edgar-lakehouse-ingest \
  --order-by LastEventTime --descending \
  --query 'logStreams[0].logStreamName' --output text | head -1)

aws logs get-log-events \
  --log-group-name /ecs/edgar-lakehouse-ingest \
  --log-stream-name "$STREAM" \
  --query 'events[].message' --output text
```

The last line of a healthy run is an `ingest_complete` event. The fields worth reading:

| field | meaning |
|---|---|
| `records`, `bytes` | how much was fetched |
| `requests` | how many calls to the SEC it took |
| `sinks` | both destinations it wrote to — cloud storage *and* the volume |
| `landing_push_failed` | `false` means the volume copy also succeeded |
| `duration_s` | wall-clock for the stream |

A real one, lightly wrapped:

```json
{"event": "ingest_complete", "stream": "company_concept",
 "logical_date": "2026-07-31", "records": 94, "bytes": 216811,
 "requests": 120, "duration_s": 27.632, "landing_push_failed": false}
```

**Searching across runs** — for example, every failure in the last day:

```bash
aws logs filter-log-events \
  --log-group-name /ecs/edgar-lakehouse-ingest \
  --start-time $(( ($(date +%s) - 86400) * 1000 )) \
  --filter-pattern '{ $.level = "error" }' \
  --query 'events[].message' --output text
```

Logs are retained for **14 days**. ECS itself only keeps stopped task records for about an
hour, so CloudWatch is the durable history, not the ECS console.

---

## 2. Processing — the Databricks job

**In the browser.** *Workflows → Jobs → `edgar-medallion-dev` → Runs.* Each run expands
into its four stages; clicking a failed stage shows the Python traceback directly.

**From the terminal.** The last dozen runs, with outcome and duration:

```bash
export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<a personal access token>

JOB=$(databricks jobs list --output json | python -c \
  "import sys,json; d=json.load(sys.stdin); \
   print([j['job_id'] for j in (d if isinstance(d,list) else d['jobs']) \
   if 'edgar-medallion' in j['settings']['name']][0])")

databricks jobs list-runs --job-id "$JOB" --limit 12 --output json
```

**Why a stage failed.** Task-level state first, then that task's output:

```bash
databricks jobs get-run <run-id> --output json      # per-task result_state
databricks jobs get-run-output <task-run-id> --output json
```

`get-run-output` returns the real Python exception. It is buried under a long Java stack
trace, so filter to the lines that matter:

```bash
databricks jobs get-run-output <task-run-id> --output json \
  | python -c "import sys,json; d=json.load(sys.stdin); \
      print('\n'.join(l for l in ((d.get('error') or '')+'\n'+(d.get('error_trace') or '')).splitlines() \
      if l.strip() and not l.strip().startswith('at ')))" | tail -40
```

A healthy run looks like this — the four stages, in order:

```
bronze_ingest       SUCCESS    62s
silver_transform    SUCCESS   116s
gold_build          SUCCESS    48s
serving_export      SUCCESS    36s
```

---

## 3. What exists, and how often it runs

Worth knowing precisely, because "how many pipelines" has a smaller answer than people
expect:

| | count | notes |
|---|---|---|
| Databricks **jobs** | **1** | `edgar-medallion-dev`, four tasks in a chain |
| Delta Live Tables **pipelines** | **0** | the medallion layers are plain job tasks, not DLT |
| SQL **warehouses** | **1** | serverless, auto-stops after 10 minutes idle |
| Model-serving **endpoints** | **0** | nothing is served from Databricks |
| ECS **scheduled rules** | **1** | fans out to 3 streams per run |
| Container **task definition** | **1** | one image, three different invocations |

So: **one job and one container**, on two schedules.

```
06:00 UTC   collection starts    (3 streams, ~30s each)
06:30 UTC   processing starts    (4 stages, ~4.5 min)
```

The 30-minute gap is the only thing sequencing them. There is no completion signal from
the collector to the pipeline — the pipeline simply reads whatever has landed by the time
it starts. That gap is deliberately generous: being early means processing a partial day
and reporting success, which is far worse than being late.

---

## 4. Run it yourself, on demand

Neither schedule has to be waited for.

**Collection** — one task per stream:

```bash
CLUSTER=edgar-lakehouse-ingest
FAMILY=$(aws ssm get-parameter --name /edgar-lakehouse/ecs/task_family \
         --query Parameter.Value --output text)

cat > ov.json <<'EOF'
{"containerOverrides":[{"name":"ingest","command":
 ["run","--stream","filing_index","--logical-date","2026-07-31","--remote","--cik-limit","8"]}]}
EOF

aws ecs run-task --cluster "$CLUSTER" --launch-type FARGATE \
  --task-definition "$FAMILY" --overrides file://ov.json \
  --network-configuration "<the awsvpc block from the schedule>"
```

Repeat with `company_submissions` and `company_concept`. Pick a **weekday** —
the SEC publishes no daily index on weekends or market holidays, and asking for one
returns `403`, not `404`.

**Processing:**

```bash
databricks bundle run edgar_medallion -t dev --params logical_date=2026-07-31
```

The parameters have defaults that work, so passing only `logical_date` is enough — that is
exactly what the scheduled run does.

**Publishing to the serving bucket** (the job writes a volume; this moves it):

```bash
python -m tools.publish_serving --dry-run   # inspect first
python -m tools.publish_serving
```

It refuses to publish a partial set. The upload uses `--delete`, so a missing table would
*remove* it from the bucket and turn one bad run into an outage for the consumer.

---

## 5. Confirm the data actually moved

```bash
RAW=$(aws ssm get-parameter --name /edgar-lakehouse/s3/raw_bucket \
      --query Parameter.Value --output text)
SERVING=$(aws ssm get-parameter --name /edgar-lakehouse/s3/serving_bucket \
          --query Parameter.Value --output text)

aws s3 ls "s3://$RAW/" --recursive --summarize | tail -2
aws s3 ls "s3://$SERVING/" --recursive
```

And the tables themselves, in the SQL editor:

```sql
SELECT COUNT(*) FROM edgar.gold.financials_current;      -- 3998
SELECT COUNT(*) FROM edgar.gold.restatement_event;       -- 329

-- the point of the whole design: one fact, more than one filed value
SELECT fact_sk, assertion_version, value, is_current_assertion
FROM   edgar.silver.financial_fact
WHERE  fact_sk IN (SELECT fact_sk FROM edgar.silver.financial_fact
                   GROUP BY fact_sk HAVING COUNT(*) > 1)
ORDER  BY fact_sk, assertion_version
LIMIT  20;
```

That last query is the one to actually run. Every row it returns is a number a company
filed and then revised, with both values still present and the current one flagged. A
pipeline that updated rows in place would return nothing.

---

## What proves it works

- Four stages green, in order, in run history.
- `landing_push_failed: false` in the collector's log — both destinations were written.
- `restatement_event` is non-empty; superseded facts are still queryable.
- Quarantine tables hold rejected rows rather than the pipeline dropping them silently.
- Re-running the same date changes no counts. The pipeline is idempotent by design: keys
  are derived from content, never from auto-incrementing numbers.
