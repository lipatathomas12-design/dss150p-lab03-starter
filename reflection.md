Section 9.5: Analysis Questions & Reflections
1. Which file format was smallest on your machine, and what encoding/compression characteristics help explain the result?

Parquet ended up being by far the smallest format compared to CSV and JSON Lines. This comes down to how it's built: it uses binary columnar storage alongside efficient compression codecs (like Snappy or Gzip), plus dictionary and run-length encoding. Text formats like CSV and JSON are inherently row-oriented and text-heavy, meaning they end up repeating structural elements like commas, quotes, and field keys for every single record, which bloats the file size.

2. Which representation was fastest for a full dataset read? Does that imply it is best for every workload?

Parquet was definitely the fastest for reading the full dataset, mainly because its columnar layout and vectorized decoding keep disk I/O to a minimum. But that doesn’t mean it’s the right tool for every job. Parquet shines for analytical processing (OLAP) where you're scanning large chunks of data, but it's terrible for workloads that need frequent row-level updates, single-record inserts, or strict transactional integrity (OLTP)—areas where a relational database like PostgreSQL naturally excels.

3. How did filtered retrieval differ between Parquet and PostgreSQL? What additional PostgreSQL design (such as an index) could change the result?

When running filtered queries, flat Parquet files generally require scanning through file blocks or relying on row-group metadata, whereas PostgreSQL leverages its built-in query optimizer and execution engine. PostgreSQL can be made even faster by adding an explicit B-tree index on the filtered column (like a CREATE INDEX statement), which lets the database perform a targeted index scan instead of a heavy full-table scan.

4. Why is JSON Lines generally more pipeline-friendly than one giant JSON array for append/stream-oriented processing?

JSON Lines (.jsonl) keeps things stream-friendly because every single record sits on its own line as a self-contained, valid JSON object. This means data pipelines can append new records just by writing to the end of the file without needing to parse, load, or rewrite the whole file. A massive JSON array, on the other hand, forces you to load the entire dataset into memory all at once and wrap it in brackets [...], making incremental streaming or appending a huge pain.

5. What happens if a partition key has extremely high cardinality or poor query locality?

If you pick a partition key with overly high cardinality—like partitioning by an exact timestamp or a unique transaction ID—you run straight into the "small file problem." Instead of having a clean set of partitions with healthy file sizes, you end up generating thousands or even millions of tiny files. That completely overwhelms file system metadata operations, slows down directory lookups, wastes storage on overhead, and kills query performance because your system spends more time opening tiny files than actually processing data.