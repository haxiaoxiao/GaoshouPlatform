"""Build and validate a compact Factor Value Store in a new directory."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from app.core.config import settings
from app.services.dataset_manifest import (
    DatasetManifest,
    read_dataset_manifest,
    write_dataset_manifest,
)

_KEY_COLUMNS = ("symbol", "trade_date", "as_of_time", "factor_name", "params_hash")


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _source_expr(files: list[Path]) -> str:
    literals = ", ".join(_sql_literal(path.as_posix()) for path in files)
    return f"read_parquet([{literals}], union_by_name=true)"


def _columns(connection: duckdb.DuckDBPyConnection, source: str) -> set[str]:
    rows = connection.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    return {str(row[0]) for row in rows}


def _canonical_sql(connection: duckdb.DuckDBPyConnection, source: str) -> tuple[str, str]:
    columns = _columns(connection, source)
    if not {"symbol", "trade_date", "value"} <= columns:
        raise ValueError(f"Factor partition is missing required columns: {sorted(columns)}")
    name_expr = "COALESCE(factor_name, feature_name)" if {"factor_name", "feature_name"} <= columns else (
        "factor_name" if "factor_name" in columns else "feature_name"
    )
    as_of_expr = "COALESCE(as_of_time, '')" if "as_of_time" in columns else "''"
    params_expr = "COALESCE(params_hash, '')" if "params_hash" in columns else "''"
    order_expr = "created_at DESC NULLS LAST" if "created_at" in columns else "trade_date DESC"
    drop_columns = [name for name in ("year", "month") if name in columns]
    select_columns = [name for name in columns if name not in drop_columns]
    projection = ", ".join(f'"{name}"' for name in sorted(select_columns))
    canonical = f"""
        SELECT {projection}
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY symbol, trade_date, {as_of_expr}, {name_expr}, {params_expr}
                ORDER BY {order_expr}
            ) AS _row_number
            FROM {source}
        )
        WHERE _row_number = 1
    """
    signature = (
        "bit_xor(hash(symbol, trade_date, "
        f"{as_of_expr}, {name_expr}, {params_expr}, value))"
    )
    return canonical, signature


def _partition_signature(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    signature_expr: str,
) -> tuple[int, int]:
    row = connection.execute(f"SELECT count(*), COALESCE({signature_expr}, 0) FROM ({query})").fetchone()
    return int(row[0] or 0), int(row[1] or 0)


def _build_factor_coverage_index(files: list[Path]) -> dict[str, dict[str, Any]]:
    if not files:
        return {}
    connection = duckdb.connect(":memory:")
    try:
        source = _source_expr(files)
        columns = _columns(connection, source)
        name_expr = (
            "COALESCE(factor_name, feature_name)"
            if {"factor_name", "feature_name"} <= columns
            else ("factor_name" if "factor_name" in columns else "feature_name")
        )
        rows = connection.execute(
            f"""
            SELECT
                {name_expr} AS factor_name,
                count(*) AS total_rows,
                count(DISTINCT symbol) AS symbol_count,
                count(DISTINCT trade_date) AS date_count,
                min(trade_date) AS min_date,
                max(trade_date) AS max_date
            FROM {source}
            WHERE {name_expr} IS NOT NULL
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()
    finally:
        connection.close()
    return {
        str(row[0]): {
            "total_rows": int(row[1] or 0),
            "symbol_count": int(row[2] or 0),
            "date_count": int(row[3] or 0),
            "min_date": str(row[4]) if row[4] is not None else None,
            "max_date": str(row[5]) if row[5] is not None else None,
        }
        for row in rows
    }


def index_compacted_factor_store(output_root: str | Path) -> dict[str, Any]:
    output = Path(output_root).resolve()
    manifest = read_dataset_manifest(output)
    if manifest is None or manifest.validation_status != "valid":
        raise ValueError("A valid compacted Factor Store manifest is required")
    output_files = sorted(output.glob("year=*/month=*/*.parquet"))
    if len(output_files) != manifest.file_count:
        raise ValueError("Compacted Factor Store file count does not match its manifest")
    factor_coverage = _build_factor_coverage_index(output_files)
    details = dict(manifest.details)
    details["factor_coverage"] = factor_coverage
    updated = replace(manifest, generated_at=datetime.now(), details=details)
    write_dataset_manifest(output, updated)
    return {
        "output": str(output),
        "validated": True,
        "factor_count": len(factor_coverage),
        "rows_after": updated.row_count,
        "manifest": str(output / "_manifest.json"),
    }


def compact_factor_value_store(
    source_root: str | Path,
    output_root: str | Path,
    *,
    resume: bool = False,
) -> dict[str, Any]:
    source = Path(source_root).resolve()
    output = Path(output_root).resolve()
    if source == output or source in output.parents:
        raise ValueError("Output directory must be separate from and outside the source directory")
    if output.exists() and any(output.iterdir()) and not resume:
        raise ValueError("Output directory must be empty")

    partitions: list[tuple[int, int, Path, list[Path]]] = []
    for year_dir in sorted(source.glob("year=*")):
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except (IndexError, ValueError):
            continue
        for month_dir in sorted(year_dir.glob("month=*")):
            try:
                month = int(month_dir.name.split("=", 1)[1])
            except (IndexError, ValueError):
                continue
            files = sorted(month_dir.glob("*.parquet"))
            if files:
                partitions.append((year, month, month_dir, files))
    if not partitions:
        raise ValueError(f"No year/month Parquet partitions found in {source}")

    output.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(":memory:")
    partition_reports: list[dict[str, Any]] = []
    min_date: str | None = None
    max_date: str | None = None
    schema_rows: list[tuple[str, str]] = []
    try:
        for year, month, _month_dir, files in partitions:
            source_expr = _source_expr(files)
            canonical, signature_expr = _canonical_sql(connection, source_expr)
            raw_count = int(connection.execute(f"SELECT count(*) FROM {source_expr}").fetchone()[0])
            expected_count, expected_signature = _partition_signature(connection, canonical, signature_expr)

            destination_dir = output / f"year={year}" / f"month={month:02d}"
            destination_dir.mkdir(parents=True, exist_ok=True)
            destination = destination_dir / "part-000.parquet"
            reused = destination.exists()
            if not reused:
                temporary = destination.with_suffix(".parquet.tmp")
                if temporary.exists():
                    temporary.unlink()
                connection.execute(
                    f"COPY ({canonical}) TO {_sql_literal(temporary.as_posix())} "
                    "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000)"
                )
                temporary.replace(destination)
            output_expr = f"read_parquet({_sql_literal(destination.as_posix())}, union_by_name=true)"
            output_canonical, output_signature_expr = _canonical_sql(connection, output_expr)
            actual_count, actual_signature = _partition_signature(
                connection,
                output_canonical,
                output_signature_expr,
            )
            if (actual_count, actual_signature) != (expected_count, expected_signature):
                action = "reused" if reused else "written"
                raise RuntimeError(f"Parity validation failed for {action} partition {year}-{month:02d}")
            bounds = connection.execute(
                f"SELECT min(trade_date), max(trade_date) FROM {output_expr}"
            ).fetchone()
            if bounds and bounds[0] is not None:
                current_min = str(bounds[0])
                current_max = str(bounds[1])
                min_date = current_min if min_date is None else min(min_date, current_min)
                max_date = current_max if max_date is None else max(max_date, current_max)
            if not schema_rows:
                schema_rows = [
                    (str(row[0]), str(row[1]))
                    for row in connection.execute(f"DESCRIBE SELECT * FROM {output_expr}").fetchall()
                ]
            partition_reports.append(
                {
                    "partition": f"{year}-{month:02d}",
                    "files_before": len(files),
                    "files_after": 1,
                    "rows_before": raw_count,
                    "rows_after": actual_count,
                    "signature": str(actual_signature),
                    "reused": reused,
                }
            )
    finally:
        connection.close()

    output_files = sorted(output.glob("year=*/month=*/*.parquet"))
    factor_coverage = _build_factor_coverage_index(output_files)
    schema_hash = hashlib.sha256(json.dumps(schema_rows, sort_keys=True).encode("utf-8")).hexdigest()
    checksum = hashlib.sha256(
        json.dumps([(item["partition"], item["rows_after"], item["signature"]) for item in partition_reports]).encode("utf-8")
    ).hexdigest()
    manifest = DatasetManifest(
        dataset="factor_values",
        generated_at=datetime.now(),
        file_count=len(output_files),
        byte_size=sum(path.stat().st_size for path in output_files),
        row_count=sum(int(item["rows_after"]) for item in partition_reports),
        partition_count=len(partition_reports),
        min_date=min_date,
        max_date=max_date,
        schema_hash=schema_hash,
        validation_status="valid",
        content_checksum=checksum,
        details={
            "source": str(source),
            "partitions": partition_reports,
            "factor_coverage": factor_coverage,
        },
    )
    write_dataset_manifest(output, manifest)
    return {
        "source": str(source),
        "output": str(output),
        "validated": True,
        "files_before": sum(len(item[3]) for item in partitions),
        "files_after": len(output_files),
        "rows_before": sum(int(item["rows_before"]) for item in partition_reports),
        "rows_after": manifest.row_count,
        "manifest": str(output / "_manifest.json"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        default=str(Path(settings.parquet_data_dir) / "factor_values"),
        help="Existing Factor Value Store directory (read-only)",
    )
    parser.add_argument("--output", required=True, help="New empty output directory")
    parser.add_argument("--report", default=None, help="Optional JSON report path")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Revalidate existing compacted months and write only missing months",
    )
    parser.add_argument(
        "--index-only",
        action="store_true",
        help="Add a coverage index to an already validated compacted store without rewriting Parquet",
    )
    args = parser.parse_args()
    report = (
        index_compacted_factor_store(args.output)
        if args.index_only
        else compact_factor_value_store(args.source, args.output, resume=args.resume)
    )
    if args.report:
        Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
