# Goal 3: Storage Interpretation
Environment: <from benchmark_environment.json: OS, machine, Python, pandas, PostgreSQL version>. Results are from my machine only.

| Format | Size (bytes) | Write median (s) | Full read median (s) | Filtered (DELIVERED) median (s) | Rows |
|---|---|---|---|---|---|
| CSV | <> | <> | <> | <> | <> |
| JSONL | <> | <> | <> | <> | <> |
| Parquet | <> | <> | <> | <> | <> |
| PostgreSQL | <pg_total_relation_size> | <> | <> | <> | <> |

1. Smallest format: <name> at <bytes>. Parquet stores columns together with dictionary/run-length encoding and snappy compression; JSONL repeats every key on every row; CSV is plain text with no types.
2. Fastest full read: <name>. That does not make it best for every workload: PostgreSQL gives concurrent access, transactions and indexes; files give cheap storage and portability.
3. Filtered retrieval: Parquet pushes the status filter into the file read; PostgreSQL scanned the table with WHERE status='DELIVERED' (<seq scan, since no index on status>). An index on status, or a partial index, could change PostgreSQL's result. Compare <my two filtered numbers>.
4. JSON Lines is one record per line, so it can be appended and streamed and a bad line doesn't break the file. One giant JSON array must be parsed completely and rewritten to add a record.
5. A partition key with very high cardinality creates many tiny files and directories (small-file problem, slow listing, poor compression); poor query locality means queries still touch most partitions, so there is no I/O saving.

Partitioning by order_year/order_month lets a query for one month read one folder instead of the whole dataset.