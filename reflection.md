# Technical Reflection

**Modularity.** Each package has one job: extract copies sources, transform handles staging and curated rules, load persists to PostgreSQL, validate asserts contracts, and benchmark only times formats. cli.py only composes them, and the Airflow DAG only calls the CLI. When a rule changes, it changes in one place.

**Idempotency.** Loads use an UPSERT on order_id. record_hash covers business content only, so rerunning the same data writes nothing. My second load reported all rows as unchanged and the duplicate check showed total equal to distinct orders.

**Storage trade-offs.** In my measurements These numbers are machine-dependent. Parquet is compact and fast for analytical column reads; PostgreSQL adds concurrency, constraints and indexes; CSV and JSONL are simple and portable but larger and untyped.

**Orchestration vs business logic.** Airflow decides when and in what order things run, with retries, timeouts and parameters. It does not contain transformation code, which keeps rules testable outside Airflow and avoids a DAG that is hard to maintain.

**AI acknowledgment.** I used generative AI as an aid for coding, debugging and writing, within the course policy, and I reviewed and can explain the submitted work.