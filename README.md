# BigQuery Partition Access Auditor

A diagnostic tool designed to identify and aggregate partition-level access patterns within BigQuery query history. This auditor utilizes abstract syntax tree (AST) analysis to detect partition pruning, including transitive propagation across join boundaries.

## Core Characteristics

### SQL AST Analysis
The auditor employs a non-destructive AST traversal to identify literal filters associated with partitioned columns. By analyzing the structure of the query rather than relying on string matching, it maintains high fidelity across complex SQL constructs such as Common Table Expressions (CTEs) and nested subqueries.

### Dimension Expansion (Gated)
The auditor supports an optional data-aware expansion mode. When enabled, it identifies high-level filters on dimension tables (e.g., month, year) and probes the underlying dimension data to resolve the specific set of partitions accessed in the fact table. This is disabled by default to minimize compute costs.

### Parallel Execution Model
To process large volumes of historical job data, the auditor utilizes a distributed parsing model. Work is partitioned across available CPU cores, minimizing the total wall-clock time required for analysis.

## Technical Design

### Architectural Data Flow

```mermaid
graph TD
    A["Environment Initialization"] --> B["Metadata Retrieval (API)"]
    B --> C["Job History Stream (INFORMATION_SCHEMA.JOBS)"]
    C --> D["Task Distribution (Process Pool)"]
    
    subgraph Parallel Workers
        D1["AST Parser 1"]
        D2["AST Parser 2"]
        Dn["AST Parser N"]
    end
    
    D1 --> E["Partial Results Aggregator"]
    D2 --> E
    Dn --> E
    
    E --> F["Identifier Normalization (YYYYMMDD)"]
    F --> G["Analytical Report"]
```

### Mathematical Foundation

#### Computation Complexity
The computational overhead of the audit process is primarily bounded by the SQL parsing phase. For a set of $Q$ queries, the time complexity $T$ can be approximated as:

$$T \approx \sum_{i=1}^{Q} O(N_i)$$

Where $N_i$ represents the number of nodes in the AST of the $i$-th query.

#### Parallel Efficiency
The system achieves speedup $S$ following Amdahl's Law, where $p$ is the parallelizable portion of the workload (SQL parsing) and $n$ is the number of processing cores:

$$S(n) = \frac{1}{(1-p) + \frac{p}{n}}$$

Given that the streaming of job history is a IO-bound sequential operation and AST parsing is a CPU-bound parallel operation, $p$ typically approaches $0.95$ for large query sets.

## Industrial Approaches to Partition Detection

Research into alternative methods for determining accessed partitions reveals several strategies, each with distinct trade-offs:

1.  **Dry Run Execution Plans**: By performing a dry run on a query, BigQuery provides a `totalBytesProcessed` estimate. While this indicates if pruning *occurred*, it does not explicitly list the partition IDs. This method is effective for forward-looking validation but requires re-executing (as dry runs) every historical query to be audited.
2.  **`INFORMATION_SCHEMA.PARTITIONS`**: This view provides metadata about existing partitions (size, row count). However, it lacks a linkage to the jobs that accessed them. 
3.  **Programmatic AST Parsing (Current Approach)**: Utilizing `INFORMATION_SCHEMA.JOBS` to retrieve historical SQL and parsing it remains the most robust metadata-only strategy. It avoids the costs and latencies of dry runs while providing granular, historical visibility into exact partition IDs.

## Usage

The auditor is executed via the command-line interface with the following options:

```bash
python3 bq_partition_audit.py --project <PROJECT_ID> --table <DATASET.TABLE> [OPTIONS]
```

### Command Line Options

| Option | Description | Default |
| :--- | :--- | :--- |
| `--project` | The Google Cloud project ID for billing and auditing. | **Required** |
| `--table` | The target table to audit (format: `project.dataset.table`). | **Required** |
| `--days` | The number of days of job history to analyze. | `7` |
| `--expand-dimensions` | Enables data-aware probing to resolve indirect dimension filters. | `False` |

### Reporting Output
The tool produces a structured summary of accessed partitions, sorted by access frequency and chronological order:

```text
Auditing: project.dataset.table
Strategy: Optimized Parallel Parsing, Streaming Fetch

Identified Partitions (ID format: YYYYMMDD):
PARTITION_ID         | ACCESS_COUNT    | CONTEXT_EX
------------------------------------------------------------
20231024             | 142             | (from d.date_col)
20231025             | 89              | (from f._PARTITIONDATE)
```

## Requirements
- Google Cloud SDK (Authenticated)
- Python 3.12+
- The execution environment is self-bootstrapping; required libraries (`sqlglot`, `pydantic`, `google-cloud-bigquery`) are automatically managed within a localized virtual environment.
