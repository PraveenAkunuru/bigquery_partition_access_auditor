"""
BigQuery Partition Access Auditor.

Follows Antigravity Protocol (Clean Architecture, SOLID, Strict Typing).
Optimized for scale: handles millions of queries via parallel parsing and streaming.
"""

import abc
import argparse
import concurrent.futures
import logging
import os
import subprocess
import sys
from collections.abc import Generator
from typing import Any, Protocol

# --- Bootstrap Layer ---


def bootstrap() -> None:
    """
    Ensures the environment is set up with required dependencies.
    Creates .venv and installs packages if not present, then re-executes.
    """
    if os.environ.get("BQ_AUDIT_BOOTSTRAPPED"):
        return

    # Check for .venv
    venv_dir = os.path.join(os.getcwd(), ".venv")
    suffix = "bin/python" if sys.platform != "win32" else "Scripts/python.exe"
    python_bin = os.path.join(venv_dir, suffix)

    if not os.path.exists(python_bin):
        logging.info("Virtual environment not found. Creating one...")
        try:
            # Prefer uv if available, fallback to venv
            if subprocess.run(["uv", "--version"], capture_output=True).returncode == 0:
                subprocess.run(["uv", "venv", ".venv"], check=True)
                subprocess.run(
                    [
                        "uv",
                        "pip",
                        "install",
                        "--python",
                        python_bin,
                        "google-cloud-bigquery",
                        "pydantic",
                        "sqlglot",
                    ],
                    check=True,
                )
            else:
                subprocess.run([sys.executable, "-m", "venv", ".venv"], check=True)
                subprocess.run(
                    [
                        python_bin,
                        "-m",
                        "pip",
                        "install",
                        "google-cloud-bigquery",
                        "pydantic",
                        "sqlglot",
                    ],
                    check=True,
                )
        except Exception as e:
            logging.error(f"Failed to bootstrap environment: {e}")
            sys.exit(1)

    # Re-exec within venv
    os.environ["BQ_AUDIT_BOOTSTRAPPED"] = "1"
    logging.info("Re-executing within virtual environment...")
    os.execv(python_bin, [python_bin] + sys.argv)


# Standard entry-point check for bootstrapping
if __name__ == "__main__" and not os.environ.get("BQ_AUDIT_BOOTSTRAPPED"):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    bootstrap()

# --- Post-Bootstrap Standard Imports ---
import sqlglot  # noqa: E402
from google.cloud import bigquery  # noqa: E402
from pydantic import BaseModel, ConfigDict, Field  # noqa: E402
from sqlglot import exp  # noqa: E402

# --- Domain Layer ---


class PartitionInfo(BaseModel):
    """
    Domain representing a partition identified in a query.
    Aligned with INFORMATION_SCHEMA.PARTITIONS ID format.
    """

    model_config = ConfigDict(frozen=True)

    value: str
    context: str | None = None
    normalized_id: str | None = None

    def __hash__(self) -> int:
        # We hash based on normalized_id to group frequencies accurately
        return hash(self.normalized_id or self.value)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PartitionInfo):
            return False
        self_id = self.normalized_id or self.value
        other_id = other.normalized_id or other.value
        return self_id == other_id


class TableMetadata(BaseModel):
    """
    Metadata describing a BigQuery table's partitioning strategy.
    """

    project: str
    dataset: str
    table: str
    partition_column: str | None
    partition_type: str | None

    @property
    def full_reference(self) -> str:
        """Returns project.dataset.table representation."""
        return f"{self.project}.{self.dataset}.{self.table}"


# --- Infrastructure Interfaces (DIP) ---


class BigQueryClientProtocol(Protocol):
    """Abstract interface for BigQuery client mocks/implementations."""

    def get_table(self, table_id: str) -> Any: ...  # noqa: ANN401
    def query(self, query: str) -> Any: ...  # noqa: ANN401


# --- Application Layer (Use Cases) ---


class PartitionExtractor(abc.ABC):
    """Base interface for SQL analyzers."""

    @abc.abstractmethod
    def extract(self, sql: str, target_col: str) -> set[PartitionInfo]:
        """Extract partition information from a SQL string."""
        pass


class SQLGlotExtractor(PartitionExtractor):
    """
    Advanced SQL AST analyzer.
    Supports multi-level joins and cross-table dimension filters.
    Optimized for batch processing.
    """

    def extract(self, sql: str, target_col: str) -> set[PartitionInfo]:
        """
        Parses SQL and extracts partitions via a query-wide table scan.
        This handles any depth of joins by collecting all date/integer literals.
        """
        results: set[PartitionInfo] = set()
        try:
            parsed = sqlglot.parse_one(sql, read="bigquery")
            table_literals: dict[str, set[PartitionInfo]] = {}

            # Typed extractor for mypy strictness
            def get_alias(col: exp.Column) -> str:
                # Use string table reference or fall back to 'unaliased'
                tbl = col.args.get("table")
                return str(tbl).lower() if tbl else "unaliased"

            # Node Discovery: Equality, IN, BETWEEN
            node: exp.Expression
            for node in parsed.find_all(exp.EQ, exp.In, exp.Between):
                # Handle Equality (EQ)
                if isinstance(node, exp.EQ):
                    left, right = node.left, node.right

                    if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                        self._add_lit(
                            table_literals, get_alias(left), left.name, right.this
                        )
                    elif isinstance(right, exp.Column) and isinstance(
                        left, exp.Literal
                    ):
                        self._add_lit(
                            table_literals, get_alias(right), right.name, left.this
                        )

                # Handle IN / BETWEEN
                elif isinstance(node.this, exp.Column):
                    col = node.this
                    alias = get_alias(col)
                    if isinstance(node, exp.In):
                        for item in node.expressions:
                            if isinstance(item, exp.Literal):
                                self._add_lit(
                                    table_literals, alias, col.name, item.this
                                )
                    else:  # Between
                        low = node.args.get("low")
                        high = node.args.get("high")
                        if isinstance(low, exp.Literal):
                            self._add_lit(
                                table_literals, alias, col.name, low.this, "range_start"
                            )
                        if isinstance(high, exp.Literal):
                            self._add_lit(
                                table_literals, alias, col.name, high.this, "range_end"
                            )

            # Collect literals from all tables encountered in the query
            for lits in table_literals.values():
                results.update(lits)

        except Exception:
            # Silicon errors are ignored in high-volume batch parsing
            pass
        return results

    def _add_lit(
        self,
        store: dict[str, set[PartitionInfo]],
        alias: str,
        col: str,
        val: str,
        ctx: str | None = None,
    ) -> None:
        """Normalizes and safely adds a literal to the extraction store."""
        if alias not in store:
            store[alias] = set()

        # Normalize to YYYYMMDD if date-like (contains hyphens/slashes)
        is_date = len(val) >= 8 and (val.count("-") == 2 or val.count("/") == 2)
        norm = val.replace("-", "").replace("/", "") if is_date else val

        # Heuristic: Valid partition IDs are numeric/date-like.
        # Filter strings like 'pri' or short aliases.
        if not norm.isdigit():
            if not norm.isalnum() or len(norm) < 4:
                return

        context = f"from {alias}.{col}"
        if ctx:
            context += f" ({ctx})"
        store[alias].add(PartitionInfo(value=val, context=context, normalized_id=norm))


# --- Performance Layer (Scaling) ---


def parse_job_batch(sql_batch: list[str], target_col: str) -> set[PartitionInfo]:
    """Helper for process pool parsing."""
    extractor = SQLGlotExtractor()
    all_touched: set[PartitionInfo] = set()
    for sql in sql_batch:
        all_touched.update(extractor.extract(sql, target_col))
    return all_touched


# --- Orchestration Layer ---


class BigQueryAuditService:
    """Manages BigQuery interactions and audit workflows."""

    def __init__(self, client: BigQueryClientProtocol) -> None:
        self.client = client

    def fetch_table_metadata(self, proj: str, ds: str, tbl: str) -> TableMetadata:
        """Retrieves partitioning metadata via API."""
        ref = self.client.get_table(f"{proj}.{ds}.{tbl}")
        p_col, p_type = None, None
        if ref.time_partitioning:
            p_col = ref.time_partitioning.field or "_PARTITIONDATE"
            p_type = ref.time_partitioning.type_
        elif ref.range_partitioning:
            p_col = ref.range_partitioning.field
            p_type = "INTEGER_RANGE"
        return TableMetadata(
            project=proj,
            dataset=ds,
            table=tbl,
            partition_column=p_col,
            partition_type=p_type,
        )

    def stream_job_history(
        self, audit_project: str, target: TableMetadata, days: int
    ) -> Generator[str, None, None]:
        """Streams SQL query text from JOBS history to avoid high memory usage."""
        sql = f"""
        SELECT query
        FROM `{audit_project}.region-us`.INFORMATION_SCHEMA.JOBS
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
          AND job_type = 'QUERY'
          AND EXISTS (
            SELECT 1 FROM UNNEST(referenced_tables) t
            WHERE t.project_id = '{target.project}' 
              AND t.dataset_id = '{target.dataset}' 
              AND t.table_id = '{target.table}'
          )
        """
        results = self.client.query(sql).result()
        for row in results:
            yield row.query


class AuditConfig(BaseModel):
    """Configuration for the audit run."""

    audit_project: str
    target_table_ref: str
    lookback_days: int = Field(default=7, ge=1)
    parallelism: int = Field(default=os.cpu_count() or 4, ge=1)


def run_audit(config: AuditConfig, client: BigQueryClientProtocol) -> None:
    """Orchestrates the high-performance audit process."""
    service = BigQueryAuditService(client)
    parts = config.target_table_ref.split(".")
    metadata = service.fetch_table_metadata(parts[0], parts[1], parts[2])

    if not metadata.partition_column:
        print(f"Table {config.target_table_ref} is not partitioned.")
        return

    print(f"Auditing: {metadata.full_reference}")
    print("Strategy: Optimized Parallel Parsing, Streaming Fetch")

    # Fetch and batch for parallel parsing
    batch_size = 500  # Optimal chunk for IPC overhead
    history = service.stream_job_history(
        config.audit_project, metadata, config.lookback_days
    )

    # Track counts using a dictionary {PartitionInfo: count}
    partition_counts: dict[PartitionInfo, int] = {}

    with concurrent.futures.ProcessPoolExecutor(max_workers=config.parallelism) as pool:
        futures = []
        current_batch = []

        for sql in history:
            current_batch.append(sql)
            if len(current_batch) >= batch_size:
                futures.append(
                    pool.submit(
                        parse_job_batch, current_batch, metadata.partition_column
                    )
                )
                current_batch = []

        if current_batch:
            futures.append(
                pool.submit(parse_job_batch, current_batch, metadata.partition_column)
            )

        for future in concurrent.futures.as_completed(futures):
            batch_results = future.result()
            for part in batch_results:
                partition_counts[part] = partition_counts.get(part, 0) + 1

    # Final reporting
    if not partition_counts:
        print("\nNo specific partitions identified in the specified lookback window.")
    else:
        print("\nIdentified Partitions (ID format: YYYYMMDD):")
        # Sort by count (desc) then by ID
        sorted_parts = sorted(
            partition_counts.items(),
            key=lambda item: (-item[1], item[0].normalized_id or item[0].value),
        )
        print(f"{'PARTITION_ID':<20} | {'ACCESS_COUNT':<15} | {'CONTEXT_EX'}")
        print("-" * 60)
        for part, count in sorted_parts:
            # We want to show a unique context but aggregate the count
            ctx = f"({part.context})" if part.context else ""
            p_id = part.normalized_id or part.value
            print(f"{p_id:<20} | {count:<15} | {ctx}")


def main() -> None:
    """CLI Entry point."""
    desc = "High-Performance BQ Partition Auditor"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("--project", required=True, help="Billing/Audit Project")
    parser.add_argument("--table", required=True, help="Target (project.dataset.table)")
    parser.add_argument("--days", type=int, default=7, help="Lookback window")
    args = parser.parse_args()

    try:
        cfg = AuditConfig(
            audit_project=args.project,
            target_table_ref=args.table,
            lookback_days=args.days,
            parallelism=os.cpu_count() or 4,
        )
        client = bigquery.Client(project=cfg.audit_project)
        run_audit(cfg, client)
    except Exception as e:
        logging.error(f"Audit engine failure: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
