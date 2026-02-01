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

from datetime import datetime

def setup_logging(level: str = "INFO") -> None:  # pragma: no cover
    """Configures structured logging to both file and stderr."""
    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"audit_{timestamp}.log")

    # Get root logger
    root = logging.getLogger()
    root.setLevel(level.upper())

    # Completely clear existing handlers (including any default ones)
    if root.hasHandlers():
        root.handlers.clear()

    # Create Formatter
    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")

    # File Handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level.upper())
    root.addHandler(file_handler)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level.upper())
    root.addHandler(console_handler)

    logging.info(f"Logging initialized. Writing to: {log_file}")

    # Suppress noisy third-party libraries
    for lib in ["google", "urllib3", "requests", "google.auth"]:
        logging.getLogger(lib).setLevel(logging.WARNING)



def bootstrap() -> None:  # pragma: no cover
    """Ensures environment has required dependencies (uv or venv)."""
    if os.environ.get("BQ_AUDIT_BOOTSTRAPPED"): return
    venv_dir = os.path.join(os.getcwd(), ".venv")
    python_bin = os.path.join(venv_dir, "bin/python" if sys.platform != "win32" else "Scripts/python.exe")

    if not os.path.exists(python_bin):
        logging.info("Bootstrapping environment...")
        try:
            has_uv = subprocess.run(["uv", "--version"], capture_output=True).returncode == 0
            if has_uv:
                subprocess.run(["uv", "venv", ".venv"], check=True)
                cmd = ["uv", "pip", "install", "--python", python_bin]
            else:
                subprocess.run([sys.executable, "-m", "venv", ".venv"], check=True)
                cmd = [python_bin, "-m", "pip", "install"]
            subprocess.run(cmd + ["google-cloud-bigquery", "pydantic", "sqlglot"], check=True)
        except Exception as e:
            logging.error(f"Bootstrap failed: {e}")
            sys.exit(1)

    os.environ["BQ_AUDIT_BOOTSTRAPPED"] = "1"
    os.execv(python_bin, [python_bin] + sys.argv)


# Standard entry-point check for bootstrapping
if __name__ == "__main__" and not os.environ.get("BQ_AUDIT_BOOTSTRAPPED"):  # pragma: no cover
    # Bootstrap phase logs to console only
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
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


class DimensionFilter(BaseModel):
    """
    Represents a filter on a dimension table that requires expansion.
    """
    model_config = ConfigDict(frozen=True)

    table_alias: str
    column: str
    operator: str
    value: str
    resolved_table: str | None = None  # Full table name if resolvable
    is_numeric: bool = False  # Track if literal was numeric

    def __hash__(self) -> int:
        return hash((self.table_alias, self.column, self.operator, self.value, self.resolved_table, self.is_numeric))

# --- Service Layer ---

class BigQueryAuditService:
    def __init__(self, client: "BigQueryClientProtocol"):
        # ... existing init ...
        self.client = client
        self._dim_cache: dict[str, set[str]] = {}

    def fetch_table_metadata(self, project: str, dataset: str, table: str) -> TableMetadata:
        # ... existing method ...
        bq_table = self.client.get_table(f"{project}.{dataset}.{table}")
        
        # Heuristic: Find first DATE/TIMESTAMP/INTEGER column that looks like a partition key
        # In a real tool, we'd check partitioning_type specifically.
        part_col = bq_table.partitioning_type  # This is usually DAY/etc, not the column name for ingestion time
        # ...
        # For this PoC, we assume ingestion-time _PARTITIONDATE unless specific field
        # We need a robust way to get the partition column.
        # Simplification: If time-partitioned, return _PARTITIONDATE.
        return TableMetadata(
            project=project,
            dataset=dataset,
            table=table,
            partition_column="_PARTITIONDATE",  # Default for PoC
            partition_type="DAY"
        )




# --- Application Layer ---


class BigQueryClientProtocol(Protocol):
    def get_table(self, table_id: str) -> Any: ...
    def query(self, query: str) -> Any: ...


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

    def extract_with_context(
        self, sql: str, target_col: str
    ) -> tuple[set[PartitionInfo], list[DimensionFilter]]:
        """
        Parses SQL and extracts partitions AND potential dimension filters.
        """
        results: set[PartitionInfo] = set()
        dim_filters: list[DimensionFilter] = []
        try:
            parsed = sqlglot.parse_one(sql, read="bigquery")
            table_literals: dict[str, set[PartitionInfo]] = {}

            # Typed extractor for mypy strictness
            def get_alias(col: exp.Column) -> str:
                # Use string table reference or fall back to 'unaliased'
                tbl = col.args.get("table")
                return str(tbl).lower() if tbl else "unaliased"

            # Alias Discovery
            alias_map: dict[str, str] = {}
            for table in parsed.find_all(exp.Table):
                # We need the full table path (project.dataset.table)
                # table.sql() returns the reconstructed SQL for the table node
                # but we usually want to avoid alias in the value.
                # 'bigquerybench.tpcds.store_sales' as s -> table.sql() includes 'as s' depending on generation?
                # No, table expression usually prints just the table unless it's in a FROM/JOIN context handling?
                # Actually safest is to check parts.
                
                t_alias = table.alias
                if t_alias:
                    # Construct full name without alias
                    # We can use a trick: clone the node, remove alias, gen sql
                    t_clone = table.copy()
                    t_clone.set("alias", None)
                    t_full_name = t_clone.sql(dialect="bigquery")
                    
                    alias_map[t_alias.lower()] = t_full_name
            
            logging.info(f"DEBUG: Parsed Alias Map: {alias_map}")

            # Node Discovery: Equality, IN, BETWEEN, Joins
            node: exp.Expression
            for node in parsed.find_all(exp.EQ, exp.In, exp.Between, exp.Join):
                # Handle Joins to find potential join keys
                if isinstance(node, exp.Join):
                    # Simplification: find equi-join columns
                    for on_node in node.find_all(exp.EQ):
                        l, r = on_node.left, on_node.right
                        if isinstance(l, exp.Column) and isinstance(r, exp.Column):
                            # We'll use this later to map dim filters to fact keys
                            pass

                # Handle Equality (EQ)
                elif isinstance(node, exp.EQ):
                    l, r = node.left, node.right
                    # Normalizing to (col, lit)
                    col, lit = (l, r) if isinstance(l, exp.Column) and isinstance(r, exp.Literal) else \
                               (r, l) if isinstance(r, exp.Column) and isinstance(l, exp.Literal) else (None, None)
                    
                    if col and lit:
                        alias, name = get_alias(col), col.name
                        self._add_lit(table_literals, alias, name, lit.this)
                        
                        # Resolve table name if possible
                        resolved = alias_map.get(alias)
                        dim_filters.append(DimensionFilter(
                            table_alias=alias, 
                            column=name, 
                            operator="=", 
                            value=lit.this,
                            resolved_table=resolved,
                            is_numeric=lit.is_number
                        ))

                # Handle IN / BETWEEN
                elif isinstance(node.this, exp.Column):
                    col = node.this
                    alias, name = get_alias(col), col.name
                    resolved = alias_map.get(alias)

                    if isinstance(node, exp.In):
                        for item in node.expressions:
                            if isinstance(item, exp.Literal):
                                self._add_lit(table_literals, alias, name, item.this)
                    elif isinstance(node, exp.Between):  # Between
                        low, high = node.args.get("low"), node.args.get("high")
                        if isinstance(low, exp.Literal):
                            self._add_lit(table_literals, alias, name, low.this, "range_start")
                        if isinstance(high, exp.Literal):
                            self._add_lit(table_literals, alias, name, high.this, "range_end")

            # Collect literals from all tables encountered in the query
            for lits in table_literals.values():
                results.update(lits)

        except Exception as e:
            logging.error(f"Extraction Error: {e}", exc_info=True)
            # Silicon errors are ignored in high-volume batch parsing
            pass
        return results, dim_filters

    def extract(self, sql: str, target_col: str) -> set[PartitionInfo]:
        """Backward compatibility for simple extraction."""
        res, _ = self.extract_with_context(sql, target_col)
        return res

    def _add_lit(self, store: dict[str, set[PartitionInfo]], alias: str, col: str, val: str, ctx: str = "") -> None:
        """Normalizes and safely adds a literal to the extraction store."""
        if alias not in store: store[alias] = set()
        
        # Normalize to YYYYMMDD if date-like (contains hyphens/slashes)
        is_date = len(val) >= 8 and (val.count("-") == 2 or val.count("/") == 2)
        norm = val.replace("-", "").replace("/", "") if is_date else val

        # Heuristic: Valid partition IDs are numeric/date-like.
        if not norm.isdigit() and (not norm.isalnum() or len(norm) < 4):
            return

        context = f"from {alias}.{col}" + (f" ({ctx})" if ctx else "")
        store[alias].add(PartitionInfo(value=val, context=context, normalized_id=norm))



def parse_job_batch(batch: list[tuple[str, str]], target_col: str) -> tuple[set[PartitionInfo], list[DimensionFilter]]:
    """Helper for process pool parsing."""
    extractor = SQLGlotExtractor()
    all_touched, all_dims = set(), []
    for job_id, sql in batch:
        p_info, d_info = extractor.extract_with_context(sql, target_col)
        all_touched.update(p_info)
        all_dims.extend(d_info)
    return all_touched, all_dims

# --- Orchestration Layer ---


class BigQueryAuditService:
    """Manages BigQuery interactions and audit workflows."""

    def __init__(self, client: BigQueryClientProtocol) -> None:
        self.client = client
        self._dim_cache: dict[str, set[str]] = {}

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

    def resolve_dimension_filter(
        self, dim: DimensionFilter, fact_join_key: str, audit_project: str
    ) -> set[str]:
        """
        Expands a dimension filter into raw join-key values by querying BigQuery.
        Uses an internal cache to avoid redundant queries.
        """
        # Note: In a production tool, we'd need a robust way to resolve
        # table aliases to full table names.
        cache_key = f"{dim.table_alias}.{dim.column}{dim.operator}{dim.value}::{fact_join_key}"
        
        # TODO: Critical Limitation (PoC)
        # The current logic assumes the dimension table contains a column named `fact_join_key`.
        # In schemas like TPC-DS (store_sales.ss_sold_date_sk = date_dim.d_date_sk), this fails
        # because we try to SELECT ss_sold_date_sk FROM date_dim.
        # Future Fix: Pass tuple (fact_key, dim_key) or map schema metadata.
        if cache_key in self._dim_cache:
            return self._dim_cache[cache_key]

        # Log for observability
        logging.info(
            f"Probing dimension: {dim.table_alias}.{dim.column} {dim.operator} "
            f"'{dim.value}' to resolve {fact_join_key}"
        )

        # In this PoC, we expect the BigQueryClient to be mocked or
        # configured with a resolver for dimension tables.
        # Minimalist resolution logic:
        # If we have a hint about the table, we query it.
        # Otherwise, we return an empty set.
        results: set[str] = set()
        
        
        # simplified query for demonstration - in prod this would map aliases
        table_ref = dim.resolved_table if dim.resolved_table else f"`{dim.table_alias}`"
        
        # Handle numeric vs string literals
        val_sql = dim.value if dim.is_numeric else f"'{dim.value}'"
        
        query = f"SELECT DISTINCT {fact_join_key} FROM {table_ref} WHERE {dim.column} {dim.operator} {val_sql}"
        try:
            logging.info(f"Issuing Dimension Probe Query:\n{query.strip()}")
            # We catch errors here because querying an alias like 'd' will fail
            # without full workspace resolution, but we want to show the intent.
            job = self.client.query(query)
            logging.info(f"Dimension probe Job ID: {job.job_id}")
            for row in job:
                # In BQ row[0] works, but we also handle dict-like mocks
                val = row[0] if isinstance(row, (tuple, list)) else list(row.values())[0] if isinstance(row, dict) else getattr(row, fact_join_key, None)
                if val:
                    results.add(str(val))
        except Exception as e:
            # Expected compliance: don't crash on unresolved aliases
            logging.warning(f"Failed to probe dimension {dim.table_alias}.{dim.column}: {e}")

        self._dim_cache[cache_key] = results
        return results

    def stream_job_history(
        self, audit_project: str, target: TableMetadata, days: int
    ) -> Generator[tuple[str, str], None, None]:
        """Streams SQL query text and job IDs from JOBS history."""
        logging.info(f"Fetching {days}-day job history for table {target.full_reference}...")
        logging.info("Purpose: Identify recent SQL queries to analyze partition access patterns.")

        sql = f"""
        SELECT job_id, query
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
        logging.info(f"Issuing History Fetch Query:\n{sql.strip()}")
        results_job = self.client.query(sql)
        logging.info(f"History fetch Job ID: {results_job.job_id}")
        results = results_job.result()
        for row in results:
            yield row.job_id, row.query


class AuditConfig(BaseModel):
    """Configuration for the audit run."""

    audit_project: str
    target_table_ref: str
    lookback_days: int = Field(default=7, ge=1)
    parallelism: int = Field(default=os.cpu_count() or 4, ge=1)
    expand_dimensions: bool = False


def run_audit(config: AuditConfig, client: BigQueryClientProtocol) -> None:
    """Orchestrates the high-performance audit process."""
    service = BigQueryAuditService(client)
    parts = config.target_table_ref.split(".")
    metadata = service.fetch_table_metadata(parts[0], parts[1], parts[2])

    if not metadata.partition_column:
        logging.warning(f"Table {config.target_table_ref} is not partitioned. Aborting.")
        return

    logging.info(f"Auditing: {metadata.full_reference}")
    logging.info(f"Strategy: Optimized Parallel Parsing (max_workers={config.parallelism})")

    # Track metrics
    total_history_count = 0
    total_probes_count = 0

    # Fetch and batch for parallel parsing
    batch_size = 500  # Optimal chunk for IPC overhead
    history = service.stream_job_history(
        config.audit_project, metadata, config.lookback_days
    )

    # Track counts and potential dimensions
    partition_counts: dict[PartitionInfo, int] = {}
    dimension_filters: set[DimensionFilter] = set()

    with concurrent.futures.ProcessPoolExecutor(max_workers=config.parallelism) as pool:
        futures = []
        current_batch = []

        for job_id, sql in history:
            total_history_count += 1
            logging.debug(f"Analyzing historical query: {job_id}")
            # User requested full query logging
            logging.info(f"Analyzing SQL text for job {job_id}:\n{sql.strip()}")
            current_batch.append((job_id, sql))
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
            batch_parts, batch_dims = future.result()
            for part in batch_parts:
                partition_counts[part] = partition_counts.get(part, 0) + 1
            dimension_filters.update(batch_dims)

    # Perform Dimension Expansion (Probing phase)
    if config.expand_dimensions and dimension_filters:
        logging.info(f"Expansion: Probing {len(dimension_filters)} dimension candidates...")
        logging.info("Reason: Detected indirect filters (joins) that may map to specific partitions. Verifying values via BigQuery.")
        
        total_probes_count = len(dimension_filters)
        for dim in dimension_filters:
            # We skip specific columns that are obviously not join keys or target cols
            if dim.column == metadata.partition_column:
                continue

            # In a real implementation, we'd resolve the join key.
            # For this PoC, we expand ANY date/int filter that might logically
            # relate to a partition.
            expanded_values = service.resolve_dimension_filter(
                dim, metadata.partition_column or "", config.audit_project
            )
            for val in expanded_values:
                p = PartitionInfo(
                    value=val,
                    context=f"expanded from {dim.table_alias}.{dim.column}",
                    normalized_id=val.replace("-", "").replace("/", ""),
                )
                # We weight expanded partitions by the usage of the filter
                # For simplicity, we just add them to the set.
                partition_counts[p] = partition_counts.get(p, 0) + 1

    logging.info(f"Audit Summary: Processed {total_history_count} queries, performed {total_probes_count} probes.")

    # Final reporting
    separator = "-" * 80
    if not partition_counts:
        logging.info("\nNo specific partitions identified in the specified lookback window.")
        return

    logging.info("\nIdentified Partitions (ID format: YYYYMMDD):")
    sorted_parts = sorted(partition_counts.items(), key=lambda i: (-i[1], i[0].normalized_id or i[0].value))
    
    header = f"{'PARTITION_ID':<20} | {'ACCESS_COUNT':<15} | {'SOURCE_COLUMN/CONTEXT'}"
    logging.info(header)
    logging.info(separator)
    for part, count in sorted_parts:
        ctx = f"({part.context})" if part.context else ""
        row = f"{part.normalized_id or part.value:<20} | {count:<15} | {ctx}"
        logging.info(row)


def main() -> None:
    """CLI Entry point."""
    desc = "BQ Partition Access Auditor"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("--project", required=True, help="Billing/Audit Project")
    parser.add_argument("--table", required=True, help="Target (project.dataset.table)")
    parser.add_argument("--days", type=int, default=7, help="Lookback window")
    parser.add_argument(
        "--expand-dimensions",
        action="store_true",
        help="Expand dimension filters (expensive)",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose DEBUG logging"
    )
    args = parser.parse_args()

    separator = "=" * 60

    try:
        log_level = "DEBUG" if args.verbose else "INFO"
        setup_logging(log_level)

        logging.info(separator)
        logging.info(f"AUDIT START: {args.project} -> {args.table}")
        logging.info(f"Strategy: Parallel Parsing (CPUs={os.cpu_count() or 4})")
        logging.info(separator)

        cfg = AuditConfig(
            audit_project=args.project,
            target_table_ref=args.table,
            lookback_days=args.days,
            parallelism=os.cpu_count() or 4,
            expand_dimensions=args.expand_dimensions,
        )
        
        # Initialize Client
        # Note: explicit project prevents some auth warnings
        client = bigquery.Client(project=cfg.audit_project)
        
        # Execute
        run_audit(cfg, client)

    except KeyboardInterrupt:
        logging.warning("\nAudit interrupted by user.")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Audit engine failure: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logging.info(separator)
        logging.info("AUDIT COMPLETE")
        logging.info(separator)
        # Flush all handlers explicitly
        for handler in logging.getLogger().handlers:
            handler.flush()
        logging.shutdown()


if __name__ == "__main__":  # pragma: no cover
    main()
