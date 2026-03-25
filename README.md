# SAS to AWS Migration Toolkit

Automated migration of 700 SAS ETL scripts to AWS **Glue (PySpark)** or **Lambda (pandas)** jobs orchestrated by Step Functions. Built for a 2-engineer, 5-month project where manual conversion alone (3.5 scripts/engineer/day) is not feasible -- so the heavy lifting is handled by AWS Bedrock foundation models that understand both SAS and PySpark/pandas.

---

## Architecture

```
                          +------------------+
                          |   sas_source/    |
                          |  700 .sas files  |
                          +--------+---------+
                                   |
                    +--------------+--------------+
                    |                             |
             +------v------+            +---------v---------+
             |  analyzer/  |            |    converter/     |
             |             |            |                   |
             | Parse       |            | BedrockConverter  |
             | Classify    |  feeds     | (Converse API)    |
             | Score       +----------->|                   |
             | Graph deps  |  tier +    | prompts.py        |
             |             |  metadata  | Jinja2 templates  |
             +------+------+            +----+----+---------+
                    |                        |    |
                    v                        |    v
           +----------------+          +-----v---------+
           |   reports/     |          | AWS Bedrock   |
           | inventory.html |          | Claude/Nova/  |
           | inventory.csv  |          | Titan/...     |
           | deps.dot/json  |          +-----+---------+
           +----------------+                |
                                   +---------+---------+
                                   | target routing    |
                                   | --target glue     |
                                   | --target lambda   |
                                   | --lambda-list     |
                                   +---+----------+----+
                                       |          |
                              +--------v---+  +---v--------------+
                              | glue_jobs/ |  | lambda_jobs/     |
                              | jobs/*.py  |  | handlers/*.py    |
                              | (PySpark)  |  | (pandas)         |
                              +--------+---+  +---+--------------+
                                       |          |
         +------------------+      +---v----------v----+
         | glue_jobs/common |      | infrastructure/   |
         | transform_utils  |      | Terraform (HCL):  |
         | (PySpark)        | used | - data_lake.tf    |
         +------------------+ at   | - glue.tf         |
                              run  | - lambda.tf       |
         +------------------+      | - orchestration.tf|
         | lambda_jobs/     |      | - monitoring.tf   |
         | common/          |      +---+----------+----+
         | transform_utils  |         |          |
         | (pandas)         |   +-----v----+ +---v-----------+
         +------------------+   | Glue Job | | Lambda Fn     |
                                | (PySpark)| | (pandas)      |
                                +-----+----+ +---+-----------+
                                      |          |
                                   +--v----------v------+
                                   | orchestration/     |
                                   | Step Functions ASL |
                                   | (Glue + Lambda     |
                                   |  mixed pipelines)  |
                                   +---------+----------+
                                             |
                              +--------------v-----------------+
                              |         AWS Runtime            |
                              |                                |
                              | S3 (raw/processed/archive)     |
                              | Glue Data Catalog              |
                              | Glue Jobs (PySpark / Spark 3.x)|
                              | Lambda Fns (pandas / Python)   |
                              | Step Functions (orchestration) |
                              | CloudWatch (dashboards+alarms) |
                              +-------+----------------+-------+
                                      |                |
                  +-------------------v--+    +--------v-----------+
                  |    Data Sources      |    | Infrastructure     |
                  |                      |    |                    |
                  | DB2 (JDBC)           |    | VPC + Subnets      |
                  | MSSQL Server (JDBC)  |    | Secrets Manager    |
                  | CSV / Excel on S3    |    | Glue Connections   |
                  +----------------------+    | VPC Endpoints      |
                                              +--------------------+
                                             |
                                   +---------v---------+
                                   |   validation/     |
                                   | Compare SAS vs    |
                                   | AWS output        |
                                   | (row counts,      |
                                   |  checksums, diffs) |
                                   +-------------------+
```

---

## How It Works

### Phase 1 -- Analyze (what are we dealing with?)

The `analyzer/` package scans every `.sas` file and produces a structured inventory:

1. **`sas_parser.py`** tokenizes each file using regex patterns, extracting `SASBlock` objects for every construct it finds: DATA steps, PROC SQL, PROC SORT, PROC MEANS, macros, %INCLUDE, LIBNAME, etc. Each block records its inputs (datasets read), outputs (datasets written), and attributes (has_merge, has_retain, has_array, has_first_last, etc.).

2. **`classifier.py`** assigns a complexity score to each file based on weighted block counts and attribute penalties. RETAIN (+5), ARRAY (+6), FIRST./LAST. (+5), and large macros (+10) drive the score up. The score maps to a tier:
   - **GREEN** (score <= 15): Fully automatable -- straightforward DATA steps and PROC SQL
   - **YELLOW** (score 16-40): Semi-automatable -- needs review after conversion
   - **RED** (score > 40): Manual conversion required -- complex macros, arrays, nested logic

3. **`dependency_graph.py`** builds a directed acyclic graph across all scripts by matching dataset producers (outputs) to consumers (inputs) and resolving %INCLUDE references. It outputs a topological execution order, a DOT graph for visualization, and a list of orphan datasets (consumed but never produced -- likely external sources).

4. **`report_generator.py`** produces an HTML dashboard and CSV with the full inventory: per-script tier, score, line count, construct counts, flags, inputs, outputs, and automation notes.

**Run it:**
```bash
sas-analyze scan --input sas_source/ --output reports/
```

### Phase 2 -- Convert (Bedrock-powered, dual target)

The `converter/` package sends each SAS file to an AWS Bedrock foundation model (Claude, Nova, or any supported model) which converts the **entire file** in one shot, preserving cross-block context such as variable references, data lineage, and macro expansions. The converter supports two compute targets:

- **Glue (PySpark)** — for large data volumes (>1 GB), complex transformations, and Spark-native workloads
- **Lambda (pandas)** — for small data volumes (<1 GB / ~1k records), simple ETL, and cost-efficient serverless execution

**How it works:**

1. **`prompts.py`** defines two comprehensive system prompts:
   - `SYSTEM_PROMPT` (Glue/PySpark) — SAS-to-PySpark mapping rules, Glue environment context, PySpark `transform_utils` signatures
   - `SYSTEM_PROMPT_LAMBDA` (Lambda/pandas) — SAS-to-pandas mapping rules, Lambda environment context, pandas `transform_utils` signatures
   Each prompt includes target-specific few-shot examples and documents every helper function available at runtime.

2. **`bedrock_converter.py`** accepts a `target` parameter (`"glue"` or `"lambda"`) that selects the appropriate system prompt and few-shot examples before sending the SAS code plus analyzer metadata to the Bedrock Converse API. It extracts Python code from the model response, compile-checks it for syntax errors, and retries with error feedback if validation fails (up to 2 retries).

3. **`transpiler.py`** orchestrates the pipeline: parse (for metadata) -> classify (for tier/score) -> Bedrock convert (with target) -> wrap in the target-specific Jinja2 template. For Glue, the template adds GlueContext initialization, `job.init()`, and `job.commit()`. For Lambda, the template adds pandas imports, an S3 client, and a `handler(event, context)` entry point.

**Supported models** (configurable via `--model`):

| Friendly Name | Bedrock Model ID |
|---|---|
| `claude-sonnet` (default) | `anthropic.claude-sonnet-4-20250514` |
| `claude-haiku` | `anthropic.claude-haiku-4-20250514` |
| `nova-pro` | `amazon.nova-pro-v1:0` |
| `nova-lite` | `amazon.nova-lite-v1:0` |

Any Bedrock model ID can also be passed directly.

The transpiler produces a conversion report categorizing every file as fully converted, needs review, or failed.

**Run it:**
```bash
# Convert all to Glue (default)
sas-convert run --input sas_source/ --output glue_jobs/jobs/

# Convert all to Lambda
sas-convert run --input sas_source/ --target lambda --output lambda_jobs/handlers/

# Mixed: specific scripts to Lambda, rest to Glue
sas-convert run --input sas_source/ --lambda-list lambda_scripts.txt

# Single file to Lambda
sas-convert single --file sas_source/my_script.sas --target lambda

# Use a different model or region
sas-convert run --input sas_source/ --model nova-pro --region us-west-2

# List available models
sas-convert models
```

The `--lambda-list` flag accepts a text file with one `.sas` filename per line. Those scripts are converted with `target="lambda"` while all others use the `--target` value (default `glue`).

### Bedrock Cost Estimate (700 scripts)

Each conversion call carries a fixed overhead (~1,400 input tokens for the system prompt and few-shot example) plus variable cost for the SAS code and generated PySpark output. The table below shows the **total one-time cost** to convert all 700 scripts, across three script-size scenarios with a 15% retry overhead.

**On-demand pricing (per 1M tokens):**

| Model | Input | Output |
|---|---|---|
| Claude Sonnet 4 (default) | $3.00 | $15.00 |
| Claude Haiku 4 | $1.00 | $5.00 |
| Nova Pro | $0.80 | $3.20 |
| Nova Lite | $0.06 | $0.24 |

**Total cost for 700 scripts:**

| Model | Small (~50 LOC avg) | Medium (~200 LOC avg) | Large (~500 LOC avg) |
|---|---|---|---|
| Claude Sonnet 4 | $7.65 | $13.65 | $26.40 |
| Claude Haiku 4 | $2.55 | $4.55 | $8.80 |
| Nova Pro | $1.84 | $3.20 | $6.08 |
| Nova Lite | $0.14 | $0.21 | $0.46 |

Add a 1.5-2x multiplier for real-world usage (prompt iteration, re-runs, validation failures). Even the most expensive scenario (Claude Sonnet 4, large scripts, 2x multiplier) stays under $55 total -- less than 1 hour of engineer time. Batch inference (available for select models) cuts these numbers in half.

**Recommended strategy:** use Claude Sonnet 4 for production runs (best code quality, fewer manual fixes) and Nova Lite for pipeline development and prompt iteration.

### AWS Infrastructure Cost Estimate (monthly)

All compute (Glue, Lambda) is **pay-per-use** -- zero cost when idle. Estimates below assume each job runs **once per day** (x 30 days/month) in `eu-west-1`.

**Assumptions:**

- **350 Glue jobs**, each running once/day = 10,500 runs/month, avg 15 min each (= 2,625 total hours)
- **350 Lambda functions**, each running once/day = 10,500 invocations/month, avg 2 min each, 2048 MB
- S3 storage: ~100 GB

**Glue cost formula:** `DPUs x total_hours x $0.44/DPU-hr`

where DPUs = workers x DPU_per_worker (G.1X = 1 DPU/worker, G.2X = 2 DPU/worker)

**Glue Jobs -- the dominant cost (350 jobs x 30 days x 15 min = 2,625 hrs/month):**

| Workers | G.1X (1 DPU/worker) | G.2X (2 DPU/worker) |
|---|---|---|
| 2 workers | 2 DPU x 2,625 hrs x $0.44 = **$2,310** | 4 DPU x 2,625 hrs x $0.44 = **$4,620** |
| 5 workers | 5 DPU x 2,625 hrs x $0.44 = **$5,775** | 10 DPU x 2,625 hrs x $0.44 = **$11,550** |
| 10 workers | 10 DPU x 2,625 hrs x $0.44 = **$11,550** | 20 DPU x 2,625 hrs x $0.44 = **$23,100** |
| 20 workers | 20 DPU x 2,625 hrs x $0.44 = **$23,100** | 40 DPU x 2,625 hrs x $0.44 = **$46,200** |

**Lambda -- 350 functions x 30 days x 2 min x 2 GB:**

- GB-seconds: 10,500 invocations x 2 GB x 120s = 2,520,000 GB-s
- Compute: 2,520,000 x $0.0000166667 = **$42.00/month**
- Requests: 10,500 x $0.20/1M = $0.002 (negligible)

**Other services (validated):**

| Service | Monthly cost | How calculated |
|---|---|---|
| S3 (100 GB storage + requests) | ~$3.00 | 100 GB x $0.023 + ~$0.70 requests |
| Glue Data Catalog (4 databases) | Free | Well under 1M objects |
| Glue Crawlers (2, ~5 min/run/day) | ~$2.20 | 2 crawlers x 5 min x 30 days = 5 DPU-hrs x $0.44 |
| Step Functions (state transitions) | ~$1.00 | ~700 jobs in ~7 layers = ~710 transitions/exec x 30 days = ~21K transitions x $0.025/1K |
| CloudWatch dashboard + alarms | ~$3.20 | 1 dashboard ($3.00) + 2 alarms (2 x $0.10) |
| CloudWatch Logs (ingestion + storage) | **~$8 - $29** | See breakdown below |
| SNS (alarm topic) | Free | Under 1M publishes |
| IAM (roles, policies) | Free | No charge |
| Lambda Layers | Free | No separate charge |
| **Other services subtotal** | **~$17 - $38** | |

**CloudWatch Logs breakdown** (Glue has `--enable-continuous-cloudwatch-log`):

| Log source | Runs/month | Log size/run | Volume | Ingestion ($0.50/GB) | Storage ($0.03/GB) |
|---|---|---|---|---|---|
| Glue jobs (conservative) | 10,500 | ~1 MB | 10.5 GB | $5.25 | $0.32 |
| Glue jobs (verbose) | 10,500 | ~5 MB | 52.5 GB | $26.25 | $1.58 |
| Lambda functions | 10,500 | ~0.5 MB | 5.3 GB | $2.63 | $0.16 |
| **Total (conservative)** | | | **15.8 GB** | **$7.88** | **$0.47** |
| **Total (verbose)** | | | **57.8 GB** | **$28.88** | **$1.74** |

Set a CloudWatch Logs retention policy (e.g., 30 days) to cap storage costs. Without retention, logs accumulate and storage grows month over month.

**Monthly total (Glue + Lambda $42 + other services ~$17-$38):**

| Workers | G.1X total | G.2X total |
|---|---|---|
| 2 workers | $2,310 + $42 + $28 = **~$2,380** | $4,620 + $42 + $28 = **~$4,690** |
| 5 workers | $5,775 + $42 + $28 = **~$5,845** | $11,550 + $42 + $28 = **~$11,620** |
| 10 workers | $11,550 + $42 + $28 = **~$11,620** | $23,100 + $42 + $28 = **~$23,170** |
| 20 workers | $23,100 + $42 + $28 = **~$23,170** | $46,200 + $42 + $28 = **~$46,270** |

*Totals above use ~$28 for other services (midpoint estimate). Actual range is $17-$38 depending on log verbosity.*

**Key takeaways:**

- **Glue is 97-99% of the total cost.** The worker count and type are the only decisions that matter for budget planning.
- **Lambda is cheap** -- $42/month for 10,500 daily invocations at 2 GB. Moving more scripts from Glue to Lambda is the single biggest cost optimization.
- **CloudWatch Logs are the second-largest cost** after Glue compute at $8-$29/month. Set a 30-day retention policy and consider disabling continuous logging for stable jobs.
- **Worker count is per-job, not shared.** Each Glue job run allocates its own workers for its duration. 2 workers at 15 min/job is the minimum viable config.
- **G.1X vs G.2X:** G.2X doubles the DPU (and cost) per worker but provides 2x memory (16 GB vs 8 GB). Use G.1X unless jobs run out of memory.
- All prices exclude AWS free-tier credits (first 12 months: 1M Lambda requests, 400K GB-s, 5 GB S3, etc.).

**Calculate your own costs:**

- [AWS Pricing Calculator](https://calculator.aws/) -- build a custom estimate for your exact workload
- [AWS Glue Pricing](https://aws.amazon.com/glue/pricing/) -- $0.44/DPU-hour, billed per second (1-min minimum)
- [AWS Lambda Pricing](https://aws.amazon.com/lambda/pricing/) -- $0.0000166667/GB-second + $0.20 per 1M requests
- [AWS CloudWatch Pricing](https://aws.amazon.com/cloudwatch/pricing/) -- $0.50/GB log ingestion, $0.03/GB-month storage
- [AWS Step Functions Pricing](https://aws.amazon.com/step-functions/pricing/) -- $0.025 per 1K state transitions
- [AWS S3 Pricing](https://aws.amazon.com/s3/pricing/) -- $0.023/GB-month (Standard), $0.005/1K PUT requests

### Data Sources

The toolkit supports three categories of data sources. The SAS parser detects which source type each script uses (database LIBNAME vs file-based LIBNAME vs PROC IMPORT DBMS=XLSX) and the Bedrock prompts include source-specific mapping rules so generated code uses the correct I/O helpers.

| Source | SAS Pattern | Glue (PySpark) | Lambda (pandas) |
|---|---|---|---|
| **DB2** | `LIBNAME lib DB2 DATABASE=...` | `read_jdbc(spark, table, secret_name)` via JDBC driver | `read_db(table, secret_name)` via SQLAlchemy + `ibm_db` |
| **MSSQL Server** | `LIBNAME lib ODBC DSN=...` | `read_jdbc(spark, table, secret_name)` via JDBC driver | `read_db(table, secret_name)` via SQLAlchemy + `pymssql` |
| **CSV on S3** | `PROC IMPORT DBMS=CSV` | `read_s3_csv(spark, s3_path)` | `pd.read_csv(s3_path)` |
| **Excel on S3** | `PROC IMPORT DBMS=XLSX` | `read_excel(spark, s3_path)` (pandas intermediate) | `read_excel(s3_path)` via `openpyxl` |
| **Parquet on S3** | (post-migration format) | `read_s3_parquet(spark, s3_path)` | `pd.read_parquet(s3_path)` |

**Database credentials** are stored in AWS Secrets Manager with this JSON structure:

```json
{
  "engine": "mssql",
  "host": "db.example.com",
  "port": 1433,
  "database": "mydb",
  "username": "svc_etl",
  "password": "..."
}
```

Both Glue and Lambda I/O utilities call `get_db_credentials(secret_name)` at runtime to fetch connection details. Secret names follow the convention `sas-migration-<env>/<db>-credentials`.

**Infrastructure required for database connectivity:**

- **VPC** with private subnets (Glue Connections and Lambda need network access to database hosts)
- **NAT Gateway** (Lambda in VPC needs it for S3 and Secrets Manager access)
- **VPC Endpoints** for S3 and Secrets Manager (cost optimization, avoids NAT for AWS service traffic)
- **Glue JDBC Connections** for DB2 and MSSQL (configured in Terraform with `enable_db2`/`enable_mssql` flags)
- **Lambda Layer** with database drivers (`pymssql`, `sqlalchemy`, `openpyxl`)

Enable database sources in Terraform:

```bash
terraform apply -var-file=environments/dev.tfvars \
  -var="enable_vpc=true" \
  -var="enable_mssql=true" \
  -var="mssql_host=db.example.com" \
  -var="mssql_database=mydb" \
  -var="mssql_username=svc_etl" \
  -var="mssql_password=secret"
```

### Phase 3 -- Shared Utilities (SAS compatibility layer)

Two parallel runtime libraries provide SAS-equivalent helper functions — one for PySpark (Glue), one for pandas (Lambda). Both expose the **same function names** so the Bedrock prompt can reference a single API regardless of the target.

**`glue_jobs/common/` (PySpark)** — imported by every generated Glue job:

- **`transform_utils.py`**:
  - `sas_date_to_spark()` / `spark_date_to_sas()` -- SAS dates are days since 1960-01-01; Spark uses 1970-01-01
  - `first_last_flags()` -- Adds `_first_<var>` / `_last_<var>` boolean columns via window functions
  - `retain_accumulate()` -- Running totals within partitions via `Window.partitionBy().orderBy()`
  - SAS function equivalents: `sas_substr`, `sas_compress`, `sas_catx`, `sas_intck`, `sas_intnx`, `sas_missing`
- **`io_utils.py`**: S3 read/write (Parquet, CSV), Glue catalog access, `read_jdbc()` for DB2/MSSQL via JDBC, `read_excel()` for Excel on S3, `get_db_credentials()` from Secrets Manager
- **`quality_utils.py`**: Null summary, type coercion, deduplication, audit columns
- **`logging_utils.py`**: Structured JSON logging for CloudWatch Logs

**`lambda_jobs/common/` (pandas)** — imported by every generated Lambda handler:

- **`transform_utils.py`**:
  - `sas_date_to_pandas()` / `pandas_date_to_sas()` -- SAS date ↔ pandas datetime via `pd.to_timedelta`
  - `first_last_flags()` -- Adds `_first_<var>` / `_last_<var>` columns via shift-based group detection
  - `retain_accumulate()` -- Running totals via `groupby().cumsum()`
  - Same SAS function equivalents using pandas `str` accessors, `pd.to_numeric`, and `pd.DateOffset`
- **`io_utils.py`**: `read_db()` for DB2/MSSQL via SQLAlchemy, `read_excel()` for Excel on S3 via openpyxl, `get_db_credentials()` from Secrets Manager

### Phase 4 -- Infrastructure as Code (Terraform)

`infrastructure/` defines the entire AWS environment in Terraform HCL, split across four resource files with per-environment variable overrides:

**`data_lake.tf`** -- S3 + Glue Catalog:
- Data bucket (versioned, AES256 encryption, glacier lifecycle for old versions, public access blocked)
- Scripts bucket (holds Glue job `.py` files and `common_utils.zip`)
- Glue Data Catalog databases: `main`, `raw`, `processed`, `archive` zones

**`glue.tf`** -- Jobs + IAM:
- IAM role for Glue with least-privilege inline policies (S3 read/write, Glue catalog, CloudWatch logs) + `AWSGlueServiceRole` managed policy
- Glue job definition (glueetl, Python 3, Glue 4.0, configurable worker type/count, Spark UI enabled)
- Glue JDBC Connections for DB2 and MSSQL (conditional via `enable_db2`/`enable_mssql`)
- Secrets Manager IAM policy for database credentials
- JDBC driver JARs (`mssql-jdbc`, `db2jcc4`) passed via `--extra-jars`
- Crawlers for raw and processed S3 zones (auto-discover schemas)

**`lambda.tf`** -- Lambda:
- IAM role for Lambda with S3 read/write, CloudWatch Logs, and Glue Catalog read permissions
- Lambda layer for pandas, numpy, s3fs, and pyarrow (Python 3.12)
- Lambda layer for database drivers (`pymssql`, `sqlalchemy`, `openpyxl`) -- conditional
- VPC configuration (subnet IDs, security group) for database connectivity
- Secrets Manager IAM policy for database credentials
- Lambda function definition (15min timeout, configurable memory, layers attached)

**`secrets.tf`** -- Secrets Manager:
- Secret resources for DB2 and MSSQL credentials (conditional via `enable_db2`/`enable_mssql`)
- JSON structure: `{engine, host, port, database, username, password}`

**`networking.tf`** -- VPC (conditional via `enable_vpc`):
- VPC with private subnets across 2 AZs
- Internet Gateway + NAT Gateway for Lambda outbound access
- VPC Endpoints for S3 (Gateway) and Secrets Manager (Interface)
- Security groups for Glue, Lambda, and VPC endpoints

**`orchestration.tf`** -- Step Functions:
- IAM role for Step Functions with Glue start/stop, Lambda invoke, and SNS publish permissions
- Sample ETL state machine (Extract -> Transform -> Load) with `.sync` integration, catch-all error handling, and X-Ray tracing

**`monitoring.tf`** -- Observability:
- CloudWatch dashboard with 4 metric widgets (Glue runs, duration, errors; Step Functions executions)
- SNS alarm topic for notifications
- Metric alarms on Glue task failures and Step Functions execution failures, both wired to SNS

**Environment configs** (`infrastructure/environments/`):
- `dev.tfvars` -- G.1X workers, 2 workers, 120min timeout
- `prod.tfvars` -- G.2X workers, 5 workers, 240min timeout

**Deploy:**
```bash
cd infrastructure
terraform init
terraform plan -var-file=environments/dev.tfvars     # preview changes
terraform apply -var-file=environments/dev.tfvars     # deploy dev
terraform apply -var-file=environments/prod.tfvars    # deploy prod
```

### Phase 5 -- Orchestration (Step Functions from dependency graph)

`orchestration/pipeline_generator.py` takes the dependency graph from the analyzer and automatically generates Step Functions ASL (Amazon States Language) definitions that support **mixed Glue + Lambda pipelines**:

1. Groups scripts into **topological layers** -- scripts in the same layer have no mutual dependencies and can run in parallel
2. Each `JobStep` has a `target` field (`"glue"` or `"lambda"`) that determines the state type:
   - Glue steps use `arn:aws:states:::glue:startJobRun.sync`
   - Lambda steps use `arn:aws:states:::lambda:invoke`
3. Each layer becomes either a single `Task` state or a `Parallel` state (multiple jobs)
4. Layers are chained sequentially: Layer 0 -> Layer 1 -> ... -> Success
5. Every task has retry logic (Glue concurrency limits / Lambda throttling) and a catch-all that routes to a `Fail` state
6. Output is a `.json` file ready to deploy via Terraform or the AWS console

### Phase 6 -- Validation (prove the migration is correct)

`validation/` uses **[DataComPy](https://github.com/capitalone/datacompy)** (Capital One's open-source SAS PROC COMPARE replacement) as its comparison engine. DataComPy supports both **pandas** and **PySpark** DataFrames natively, matching the dual Glue/Lambda architecture:

- **`mode="pandas"`** (default) -- uses `datacompy.core.Compare`; no Spark/Java needed, ideal for Lambda outputs and local development
- **`mode="spark"`** -- uses `datacompy.spark.sql.SparkSQLCompare`; for Glue outputs that require a SparkSession

What it compares:

1. **Row count** -- exact match required
2. **Schema** -- column names compared; reports missing/extra columns
3. **Join-based row matching** -- DataComPy joins on key columns and compares every value, respecting configurable absolute and relative tolerances
4. **Column-level mismatch detection** -- identifies which specific columns have value differences, with counts and max diff
5. **Unmatched rows** -- rows present in SAS but not AWS (and vice versa) are surfaced separately
6. **Full-text report** -- DataComPy generates a detailed human-readable report (identical to SAS PROC COMPARE output) that can be saved with `--full-report`
7. **Golden snapshots** -- capture SAS output metrics (row count, null counts, distinct counts per column, sample rows as Parquet) before decommissioning SAS for later comparison

**Run it:**
```bash
# Pandas mode (default, no Spark needed)
sas-validate compare --sas-output golden/ --spark-output data/aws_output/ --mode pandas

# Spark mode (for Glue outputs)
sas-validate compare --sas-output golden/ --spark-output data/spark_output/ --mode spark

# Save the full DataComPy report
sas-validate compare --sas-output golden/ --spark-output data/ --full-report reports/datacompy_report.txt

# Golden snapshots (works in both modes)
sas-validate snapshot --input data/sas_output/ --output golden/
sas-validate snapshot --input data/sas_output/ --output golden/ --mode spark
```

---

## CI/CD Pipeline

GitHub Actions (`.github/workflows/ci.yml`) implements a full deployment pipeline:

```
git push
    |
    v
[Lint] ruff check + ruff format --check + terraform fmt -check + terraform validate
    |
    v
[Test] pytest (analyzer + converter + validation tests, 114 tests)
    |
    v
[Deploy Dev]  (on push to develop)
    |-- aws s3 sync glue_jobs/ -> S3 scripts bucket
    |-- zip common/ -> common_utils.zip -> S3
    |-- terraform init -> plan -> apply (-var-file=environments/dev.tfvars)
    |
    v
[Deploy Prod] (on push to main, requires environment approval)
    |-- same as dev but with -var-file=environments/prod.tfvars
```

A separate workflow (`.github/workflows/analyze.yml`) can be triggered manually to run the analyzer and converter on a batch of SAS files, uploading reports as CI artifacts.

---

## Project Structure

```
claude_46_opus_high/
    analyzer/
        sas_parser.py           # Regex-based SAS tokenizer (16+ block types)
        classifier.py           # GREEN/YELLOW/RED scoring engine
        dependency_graph.py     # DAG builder with topological sort
        report_generator.py     # HTML dashboard + CSV inventory
        cli.py                  # sas-analyze CLI
        tests/
            test_analyzer.py    # 15 tests (parser, classifier, graph)
            test_db_sources.py  # 22 tests (DB LIBNAME detection, prompt content, io_utils)
    converter/
        transpiler.py           # Core engine: parse -> Bedrock -> assemble (glue/lambda)
        bedrock_converter.py    # AWS Bedrock Converse API client + retry logic
        prompts.py              # System prompts (Glue + Lambda), few-shot examples
        cli.py                  # sas-convert CLI (--target, --lambda-list, --model)
        templates/
            glue_job.py.j2          # Jinja2 Glue (PySpark) job skeleton
            lambda_handler.py.j2    # Jinja2 Lambda (pandas) handler skeleton
        tests/
            test_bedrock_converter.py  # 30 tests (prompts, extraction, retry, e2e)
            test_transpiler.py         # 12 tests (conversion, syntax validity)
            test_lambda_target.py      # 16 tests (Lambda prompts, routing, templates)
    glue_jobs/
        common/
            transform_utils.py  # PySpark: SAS date, RETAIN, FIRST/LAST, INTCK/INTNX
            io_utils.py         # S3, Glue catalog, JDBC (DB2/MSSQL), Excel I/O
            quality_utils.py    # Nulls, types, dedup, audit columns
            logging_utils.py    # Structured JSON logging for CloudWatch
        jobs/                   # Generated PySpark Glue jobs land here
    lambda_jobs/
        common/
            transform_utils.py  # pandas: SAS date, RETAIN, FIRST/LAST, INTCK/INTNX
            io_utils.py         # DB2/MSSQL (SQLAlchemy), Excel (openpyxl), Secrets Manager
        handlers/               # Generated pandas Lambda handlers land here
    infrastructure/
        main.tf                 # Provider, backend, locals
        variables.tf            # Environment variables (inc. DB/VPC config)
        outputs.tf              # Exported resource IDs/ARNs
        data_lake.tf            # S3 buckets + Glue catalog databases
        glue.tf                 # Glue IAM + job + crawlers + JDBC connections
        lambda.tf               # Lambda IAM + function + pandas/db layers + VPC
        secrets.tf              # Secrets Manager for DB2/MSSQL credentials
        networking.tf           # VPC, subnets, NAT, VPC endpoints, security groups
        orchestration.tf        # Step Functions IAM + state machine (Glue + Lambda)
        monitoring.tf           # SNS + CloudWatch dashboard + alarms
        environments/
            dev.tfvars          # Dev environment overrides
            prod.tfvars         # Prod environment overrides
    orchestration/
        pipeline_generator.py   # Dependency graph -> Step Functions ASL (Glue + Lambda)
        templates/
            etl_pipeline.json   # Reusable ETL pipeline template
    validation/
        comparator.py           # DataComPy-powered comparison engine (pandas + Spark)
        cli.py                  # sas-validate CLI (--mode pandas/spark, --full-report)
        tests/
            test_comparator.py  # 16 tests (pandas comparison, tolerance, golden snapshots)
    sas_source/                 # Drop your .sas files here
    .github/workflows/
        ci.yml                  # Lint -> Test -> Deploy (dev/prod)
        analyze.yml             # Manual: run analyzer + converter
    pyproject.toml              # Package config, deps, CLI entry points
```

---

## Quick Start

```bash
# 1. Set up environment
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 2. Configure AWS credentials (Bedrock access required for conversion)
export AWS_PROFILE=your-profile          # or use aws configure
# Ensure your IAM user/role has bedrock:InvokeModel permission

# 3. Drop SAS files into sas_source/

# 4. Analyze
sas-analyze scan --input sas_source/ --output reports/
# -> Open reports/inventory.html for the full dashboard

# 5. Convert to Glue (default)
sas-convert run --input sas_source/ --output glue_jobs/jobs/

# 5b. Convert to Lambda (small-data scripts)
sas-convert run --input sas_source/ --target lambda

# 5c. Mixed: some to Lambda, rest to Glue
echo -e "small_script_a.sas\nsmall_script_b.sas" > lambda_scripts.txt
sas-convert run --input sas_source/ --lambda-list lambda_scripts.txt

# -> Check reports/conversion_report.csv for per-file status and target

# 6. Deploy infrastructure
cd infrastructure
terraform init
terraform apply -var-file=environments/dev.tfvars

# 7. Validate after running jobs (pandas mode, no Spark needed)
sas-validate compare --sas-output golden/ --spark-output data/aws_output/ --mode pandas

# 7b. Validate with Spark mode and full DataComPy report
sas-validate compare --sas-output golden/ --spark-output s3://bucket/processed/ --mode spark --full-report reports/datacompy.txt
```

---

## Test Suite

114 tests covering the analyzer, Bedrock converter, transpiler, Lambda target, DataComPy validation, and database source detection:

```bash
python -m pytest analyzer/tests/ converter/tests/ validation/tests/ -v
# 114 tests total (15 analyzer + 25 data sources + 30 bedrock + 12 transpiler + 16 lambda + 16 validation)
```

- **Data source tests (22)**: DB2/ODBC LIBNAME detection in parser, PROC IMPORT DBMS=XLSX detection, PROC SQL CONNECT TO detection, Bedrock prompt content for database helpers (Glue + Lambda), Glue `io_utils` import verification (`read_jdbc`, `read_excel`, `get_db_credentials`), Lambda `io_utils` import verification (`read_db`, `read_excel`, `get_db_credentials`), `build_connection_url` for MSSQL/DB2, unsupported engine error handling.
- **Analyzer tests (15)**: SAS parsing of DATA steps, PROC SQL, MERGE, RETAIN, FIRST./LAST., macros, arrays, %INCLUDE; GREEN/YELLOW/RED classification accuracy; dependency graph construction.
- **Bedrock converter tests (30)**: Model registry, prompt construction (SAS code inclusion, metadata, few-shot examples), code extraction from markdown fences, syntax validation, mocked Converse API integration (throttle retry, syntax error retry, retry exhaustion, non-retryable errors), end-to-end transpile with mocked Bedrock (single file, batch directory, valid Python output, graceful failure).
- **Transpiler tests (12)**: End-to-end conversion of simple/medium/complex SAS scripts with mocked Bedrock responses; Glue boilerplate presence; filter/orderBy/join generation; Python syntax validity of all generated code.
- **Lambda target tests (16)**: Lambda prompt content (pandas references, no Glue references), few-shot example validation, `build_conversion_prompt` label routing, Bedrock system prompt selection by target, few-shot example routing, result `target` field, `code_blocks`/`pyspark_blocks` backward compatibility, Lambda template rendering (pandas imports, handler signature, no Glue boilerplate), generated code compilation, batch `--lambda-list` routing, and `default_target` propagation.
- **Validation tests (16)**: DataComPy initialization (pandas/spark modes, backward-compatible alias), exact-match comparison, row-count mismatch detection, column/schema mismatch detection (missing + extra columns), value mismatch detection with per-column flagging, unmatched row detection (rows only in SAS/AWS), tolerance behavior (within-tolerance pass, outside-tolerance fail), `ColumnDiff` backward-compatible property aliases, `ComparisonResult` defaults, auto key-column fallback, golden snapshot generation (JSON + Parquet output).

All Bedrock API calls are mocked via `unittest.mock` and all validation tests use in-memory pandas DataFrames, so the entire suite runs offline and deterministically.
