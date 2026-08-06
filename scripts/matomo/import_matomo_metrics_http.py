#!/usr/bin/env python3
"""
Dump metrics from Matomo (Piwik) as a SQL file for the Hydra CSV database.

Fetches page view data from the Matomo API at dados.gov.pt/stats/, resolves the
slugs to ObjectIds against MongoDB, and writes a portable SQL dump with the
metric schema rows (visits_datasets, visits_resources, visits_reuses,
visits_organizations, visits_dataservices).

No PostgreSQL connection is made: the output is a plain SQL file that can be
copied to another machine and loaded there with psql. Pass --import only if you
explicitly want the legacy behaviour of writing straight into the database.

Based on: https://github.com/datagouv/datagouvfr_data_pipelines/tree/main/dgv/metrics

Settings can be overridden with environment variables (MATOMO_URL, MATOMO_TOKEN,
MATOMO_SITE_ID, MONGO_HOST, MONGO_PORT, MONGO_DB, METRICS_DUMP_DIR, and PG_* for
--import).

Usage:
    # Dump yesterday's metrics to scripts/dumps/matomo-metrics-<date>.sql
    python3 scripts/import_matomo_metrics.py

    # Dump a specific date to a chosen file
    python3 scripts/import_matomo_metrics.py --date 2026-03-15 --output ./metrics.sql

    # Dump the last 30 days
    python3 scripts/import_matomo_metrics.py --date 2026-03-20 --days 30

    # Dump ALL historical data from Matomo (since 2018-07-19)
    python3 scripts/import_matomo_metrics.py --all --output ./metrics-full.sql

    # Data only, without CREATE SCHEMA/TABLE/INDEX statements
    python3 scripts/import_matomo_metrics.py --no-schema

    # Then, on the target machine:
    #   psql -h HOST -p 5434 -U postgres -d postgres -f ./metrics.sql

    # Legacy: write directly into PostgreSQL instead of producing a dump
    python3 scripts/import_matomo_metrics.py --import
"""

import argparse
import os
import re
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TextIO

import requests
from pymongo import MongoClient

# Matomo config (from backend .env)
MATOMO_URL = os.environ.get("MATOMO_URL", "http://10.50.37.53/stats/")
MATOMO_TOKEN = os.environ.get("MATOMO_TOKEN", "2a62abafa550d3aaba8c7a6a4bd1769b")
MATOMO_SITE_ID = int(os.environ.get("MATOMO_SITE_ID", 3))

# MongoDB config (for slug -> ObjectId resolution)
MONGO_HOST = os.environ.get("MONGO_HOST", "10.55.37.143")
MONGO_PORT = int(os.environ.get("MONGO_PORT", 27017))
MONGO_DB = os.environ.get("MONGO_DB", "udata")

# PostgreSQL config (hydra_postgres_csv) — only used by --import
PG_HOST = os.environ.get("PG_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("PG_PORT", 5434))
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "postgres")
PG_DB = os.environ.get("PG_DB", "postgres")

# SQL File path for initialization (used by --import)
SQL_FILE = os.path.join(os.path.dirname(__file__), "create_tables.sql")

# Dump defaults
DUMP_DIR = Path(os.environ.get("METRICS_DUMP_DIR", Path(__file__).parent / "dumps"))
SCHEMA = "metric"
INSERT_BATCH_SIZE = 500

# Target table per object type: (table, id column, extra id columns)
TABLES: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "datasets": ("visits_datasets", "dataset_id", ("organization_id",)),
    "reuses": ("visits_reuses", "reuse_id", ("organization_id",)),
    "organizations": ("visits_organizations", "organization_id", ()),
    "dataservices": ("visits_dataservices", "dataservice_id", ("organization_id",)),
    "resources": ("visits_resources", "resource_id", ("dataset_id", "organization_id")),
}

# Materialized views refreshed at the end of the dump, when they exist.
MATERIALIZED_VIEWS = [
    "metrics_datasets",
    "metrics_reuses",
    "metrics_dataservices",
    "metrics_organizations",
    "datasets",
    "datasets_total",
    "resources",
    "resources_total",
    "organizations",
    "organizations_total",
    "reuses",
    "reuses_total",
    "dataservices",
    "dataservices_total",
    "site",
]

# URL patterns to extract object type and ID
PATTERNS = {
    "datasets": re.compile(r"/(?:pt|en|fr|es)/datasets/([a-z0-9][a-z0-9-]*[a-z0-9])(?:/|$)"),
    "reuses": re.compile(r"/(?:pt|en|fr|es)/reuses/([a-z0-9][a-z0-9-]*[a-z0-9])(?:/|$)"),
    "organizations": re.compile(
        r"/(?:pt|en|fr|es)/organizations/([a-z0-9][a-z0-9-]*[a-z0-9])(?:/|$)"
    ),
    "dataservices": re.compile(
        r"/(?:pt|en|fr|es)/dataservices/([a-z0-9][a-z0-9-]*[a-z0-9])(?:/|$)"
    ),
    "resources": re.compile(r"/(?:pt|en|fr|es)/datasets/r/([a-f0-9-]{36}|[a-f0-9]{24})(?:/|$)"),
}


def matomo_api(method: str, date: str, **extra_params) -> list | dict:
    """Call Matomo API and return JSON response."""
    params = {
        "module": "API",
        "method": method,
        "idSite": MATOMO_SITE_ID,
        "period": "day",
        "date": date,
        "format": "JSON",
        "token_auth": MATOMO_TOKEN,
        "expanded": 1,
        "flat": 1,
        "filter_limit": -1,
        **extra_params,
    }
    resp = requests.get(MATOMO_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and data.get("result") == "error":
        print(f"  Matomo error: {data.get('message')}", file=sys.stderr)
        return []
    return data


def build_slug_to_oid_lookup() -> dict[tuple[str, str], dict]:
    """Build slug -> ObjectId mapping from MongoDB."""
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    lookup = {}

    # Datasets
    for doc in db["dataset"].find({"deleted": None}, {"slug": 1, "organization": 1}):
        oid = str(doc["_id"])
        lookup[("datasets", doc.get("slug", ""))] = {
            "id": oid,
            "organization_id": str(doc["organization"]) if doc.get("organization") else None,
        }
        lookup[("datasets", oid)] = lookup[("datasets", doc.get("slug", ""))]

    # Resources -> dataset_id
    for doc in db["dataset"].find(
        {"resources": {"$exists": True}, "deleted": None},
        {"resources._id": 1, "organization": 1},
    ):
        ds_id = str(doc["_id"])
        org_id = str(doc["organization"]) if doc.get("organization") else None
        for res in doc.get("resources", []):
            lookup[("resources", str(res["_id"]))] = {
                "id": str(res["_id"]),
                "dataset_id": ds_id,
                "organization_id": org_id,
            }

    # Organizations
    for doc in db["organization"].find({"deleted": None}, {"slug": 1}):
        oid = str(doc["_id"])
        lookup[("organizations", doc.get("slug", ""))] = {"id": oid}
        lookup[("organizations", oid)] = {"id": oid}

    # Reuses
    for doc in db["reuse"].find({"deleted": None}, {"slug": 1, "organization": 1}):
        oid = str(doc["_id"])
        lookup[("reuses", doc.get("slug", ""))] = {
            "id": oid,
            "organization_id": str(doc["organization"]) if doc.get("organization") else None,
        }
        lookup[("reuses", oid)] = lookup[("reuses", doc.get("slug", ""))]

    # Dataservices
    if "dataservice" in db.list_collection_names():
        for doc in db["dataservice"].find({"deleted": None}, {"slug": 1, "organization": 1}):
            oid = str(doc["_id"])
            lookup[("dataservices", doc.get("slug", ""))] = {
                "id": oid,
                "organization_id": str(doc["organization"]) if doc.get("organization") else None,
            }
            lookup[("dataservices", oid)] = lookup[("dataservices", doc.get("slug", ""))]

    client.close()
    return lookup


def extract_visits_from_matomo(date: str) -> dict[str, dict[str, int]]:
    """Extract page views from Matomo and group by object type."""
    print(f"  Fetching page URLs from Matomo for {date}...")
    pages = matomo_api("Actions.getPageUrls", date)

    visits = defaultdict(lambda: defaultdict(int))

    for page in pages:
        label = page.get("label", "")
        nb_visits = page.get("nb_visits", 0)

        for obj_type, pattern in PATTERNS.items():
            match = pattern.search(label)
            if match:
                slug = match.group(1)
                visits[obj_type][slug] += nb_visits
                break

    for obj_type, data in visits.items():
        print(f"    {obj_type}: {len(data)} objects, {sum(data.values())} total visits")

    return visits


def extract_outlinks_from_matomo(date: str) -> dict[str, int]:
    """Extract outlinks from Matomo for datasets (matomo_* tables)."""
    print(f"  Fetching outlinks from Matomo for {date}...")
    outlinks = matomo_api("Actions.getOutlinks", date)

    results = defaultdict(int)
    for link in outlinks:
        nb_hits = link.get("nb_hits", 0)
        label = link.get("label", "")
        if label:
            results[label] += nb_hits

    print(f"    {len(results)} outlinks found")
    return results


def resolve_rows(
    visits: dict[str, dict[str, int]], date: str, lookup: dict
) -> dict[str, list[tuple]]:
    """Turn Matomo slugs into database rows, grouped by target table.

    Rows follow the column order ``(date_metric, <id_col>, *extra_cols, nb_visit)``.
    Slugs missing from the MongoDB lookup are skipped (deleted or unknown objects).
    """
    rows: dict[str, list[tuple]] = defaultdict(list)
    skipped = 0

    for obj_type, slugs in visits.items():
        if obj_type not in TABLES:
            continue
        table, _, extra_cols = TABLES[obj_type]

        for slug, nb_visit in slugs.items():
            info = lookup.get((obj_type, slug))
            if not info:
                skipped += 1
                continue
            values = [date, info["id"]]
            values += [info.get(col) for col in extra_cols]
            values.append(nb_visit)
            rows[table].append(tuple(values))

    if skipped:
        print(f"    {skipped} objects skipped (not found in MongoDB)")
    return rows


def sql_literal(value: object) -> str:
    """Render a Python value as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def write_schema_ddl(out: TextIO) -> None:
    """Write CREATE SCHEMA/TABLE/INDEX statements for the visits_* tables."""
    out.write(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};\n\n")
    out.write("-- Tables are only created when missing, so an existing schema is left untouched.\n")
    for table, id_col, extra_cols in TABLES.values():
        columns = ["date_metric date NOT NULL", f"{id_col} text NOT NULL"]
        columns += [f"{col} text" for col in extra_cols]
        columns.append("nb_visit integer NOT NULL DEFAULT 0")
        body = ",\n    ".join(columns)
        out.write(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{table} (\n    {body}\n);\n")

    out.write("\n-- Unique indexes backing the ON CONFLICT clauses below.\n")
    for table, id_col, _ in TABLES.values():
        out.write(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {table}_upsert_idx "
            f"ON {SCHEMA}.{table} ({id_col}, date_metric);\n"
        )
    out.write("\n")


def write_rows(
    out: TextIO,
    table: str,
    id_col: str,
    extra_cols: tuple[str, ...],
    rows: list[tuple],
) -> None:
    """Write batched INSERT ... ON CONFLICT statements for one table."""
    columns = ["date_metric", id_col, *extra_cols, "nb_visit"]
    column_list = ", ".join(columns)

    for start in range(0, len(rows), INSERT_BATCH_SIZE):
        batch = rows[start : start + INSERT_BATCH_SIZE]
        values = ",\n    ".join(
            "(" + ", ".join(sql_literal(value) for value in row) + ")" for row in batch
        )
        out.write(
            f"INSERT INTO {SCHEMA}.{table} ({column_list}) VALUES\n    {values}\n"
            f"ON CONFLICT ({id_col}, date_metric) DO UPDATE SET nb_visit = EXCLUDED.nb_visit;\n"
        )


def write_refresh_block(out: TextIO) -> None:
    """Write a guarded REFRESH MATERIALIZED VIEW block (skips views that don't exist)."""
    views = ", ".join(f"'{view}'" for view in MATERIALIZED_VIEWS)
    out.write(
        "-- Refresh the materialized views that exist on the target, in dependency order.\n"
        "DO $$\n"
        "DECLARE\n"
        "    view_name text;\n"
        "BEGIN\n"
        f"    FOREACH view_name IN ARRAY ARRAY[{views}]::text[] LOOP\n"
        "        IF EXISTS (\n"
        "            SELECT 1 FROM pg_matviews\n"
        f"            WHERE schemaname = '{SCHEMA}' AND matviewname = view_name\n"
        "        ) THEN\n"
        f"            EXECUTE format('REFRESH MATERIALIZED VIEW {SCHEMA}.%I', view_name);\n"
        "        END IF;\n"
        "    END LOOP;\n"
        "END $$;\n"
    )


def build_dump_path(output: str | None, dump_dir: Path, dates: list[str]) -> Path:
    """Resolve the output path of the dump."""
    if output:
        return Path(output).expanduser()

    span = dates[0] if len(dates) == 1 else f"{dates[0]}_{dates[-1]}"
    return Path(dump_dir).expanduser() / f"matomo-metrics-{span}.sql"


def dump_to_sql(
    dates: list[str],
    lookup: dict,
    output: str | None = None,
    dump_dir: Path = DUMP_DIR,
    with_schema: bool = True,
    with_refresh: bool = True,
) -> Path:
    """Fetch the metrics for each date and write them as a SQL dump.

    The file is written incrementally (one date at a time) so long ranges do not
    have to be held in memory, and is removed if anything fails halfway through.

    Args:
        dates: Dates to fetch, oldest first, as ``YYYY-MM-DD``.
        lookup: slug -> ObjectId mapping from :func:`build_slug_to_oid_lookup`.
        output: Explicit output path. Defaults to a name derived from the dates.
        dump_dir: Directory used when ``output`` is not given.
        with_schema: Include CREATE SCHEMA/TABLE/INDEX statements.
        with_refresh: Include the REFRESH MATERIALIZED VIEW block.

    Returns:
        The path of the dump file.
    """
    path = build_dump_path(output, dump_dir, dates)
    path.parent.mkdir(parents=True, exist_ok=True)

    totals: dict[str, int] = defaultdict(int)
    generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

    try:
        with path.open("w", encoding="utf-8") as out:
            out.write(
                f"-- Matomo metrics dump for dados.gov.pt\n"
                f"-- Generated at: {generated_at}\n"
                f"-- Source: {MATOMO_URL} (site {MATOMO_SITE_ID})\n"
                f"-- Dates: {dates[0]} .. {dates[-1]} ({len(dates)} day(s))\n"
                f"-- Restore with: psql -h HOST -p PORT -U USER -d DB -f {path.name}\n\n"
                "BEGIN;\n\n"
            )

            if with_schema:
                write_schema_ddl(out)

            for date in dates:
                print(f"\n=== Processing {date} ===")
                visits = extract_visits_from_matomo(date)
                rows = resolve_rows(visits, date, lookup)

                day_rows = sum(len(table_rows) for table_rows in rows.values())
                out.write(f"-- {date}: {day_rows} row(s)\n")
                for obj_type, (table, id_col, extra_cols) in TABLES.items():
                    table_rows = rows.get(table, [])
                    if not table_rows:
                        continue
                    write_rows(out, table, id_col, extra_cols, table_rows)
                    totals[table] += len(table_rows)
                out.write("\n")
                print(f"  Wrote {day_rows} rows to the dump")

            if with_refresh:
                write_refresh_block(out)

            out.write("\nCOMMIT;\n")
    except BaseException:
        # Never leave a half-written dump behind: it would look like a valid one.
        path.unlink(missing_ok=True)
        raise

    print("\nDump summary:")
    for table, count in sorted(totals.items()):
        print(f"  {SCHEMA}.{table}: {count} rows")
    size_mib = path.stat().st_size / (1024 * 1024)
    print(f"  Total: {sum(totals.values())} rows, {size_mib:.1f} MiB")
    print(f"\nDump written: {path}")
    print(
        f"  Load it on the target machine with:\n    psql -h HOST -p PORT -U USER -d DB -f {path}"
    )
    return path


def connect_postgres():
    """Connect to PostgreSQL (only needed by --import)."""
    try:
        import psycopg2
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise RuntimeError(
            "psycopg2 is required for --import (not needed to produce a dump)."
        ) from exc

    print(f"Connecting to PostgreSQL {PG_HOST}:{PG_PORT}...")
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB
    )


def save_rows_to_postgres(conn, rows: dict[str, list[tuple]]) -> int:
    """Insert resolved rows into PostgreSQL (used by --import)."""
    cur = conn.cursor()
    total = 0

    for table, id_col, extra_cols in TABLES.values():
        table_rows = rows.get(table, [])
        if not table_rows:
            continue
        columns = ["date_metric", id_col, *extra_cols, "nb_visit"]
        column_list = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        cur.executemany(
            f"""
            INSERT INTO {SCHEMA}.{table} ({column_list})
            VALUES ({placeholders})
            ON CONFLICT ({id_col}, date_metric)
            DO UPDATE SET nb_visit = EXCLUDED.nb_visit
            """,
            table_rows,
        )
        total += len(table_rows)

    conn.commit()
    cur.close()
    print(f"  Inserted/updated {total} visit records")
    return total


def refresh_materialized_views(conn) -> None:
    """Refresh all materialized views in the correct order (used by --import)."""
    print("  Refreshing materialized views...")
    cur = conn.cursor()
    for view in MATERIALIZED_VIEWS:
        cur.execute(f"REFRESH MATERIALIZED VIEW {SCHEMA}.{view}")
    conn.commit()
    cur.close()
    print(f"  Refreshed {len(MATERIALIZED_VIEWS)} materialized views")


def ensure_upsert_indexes(conn) -> None:
    """Create unique indexes needed for upsert operations (used by --import)."""
    cur = conn.cursor()
    for table, id_col, _ in TABLES.values():
        cur.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {table}_upsert_idx "
            f"ON {SCHEMA}.{table} ({id_col}, date_metric)"
        )
    conn.commit()
    cur.close()


def ensure_database_setup(conn) -> None:
    """Execute the create_tables.sql file to ensure schema and tables exist."""
    print(f"  Ensuring database schema and tables (from {os.path.basename(SQL_FILE)})...")
    if not os.path.exists(SQL_FILE):
        print(f"    Warning: SQL file {SQL_FILE} not found. Skipping full setup.")
        return

    cur = conn.cursor()
    with open(SQL_FILE, "r") as f:
        sql_content = f.read()

    try:
        cur.execute(sql_content)
        conn.commit()
        print("    Database setup finished successfully!")
    except Exception as e:
        conn.rollback()
        print(f"    Error during database setup: {e}")
    finally:
        cur.close()


def import_to_postgres(dates: list[str], lookup: dict) -> None:
    """Legacy path: write the metrics straight into PostgreSQL."""
    conn = connect_postgres()
    try:
        ensure_database_setup(conn)
        ensure_upsert_indexes(conn)

        for date in dates:
            print(f"\n=== Processing {date} ===")
            visits = extract_visits_from_matomo(date)
            save_rows_to_postgres(conn, resolve_rows(visits, date, lookup))

        refresh_materialized_views(conn)
    finally:
        conn.close()


def resolve_dates(args: argparse.Namespace) -> list[str]:
    """Build the list of dates to fetch, oldest first."""
    if args.all:
        end_date = datetime.now(UTC) - timedelta(days=1)
        # Matomo site created on 2018-07-19
        matomo_start = datetime(2018, 7, 19, tzinfo=UTC)
        days = (end_date - matomo_start).days
        print(f"Dumping ALL data: {days} days (from {matomo_start.date()} to {end_date.date()})")
    else:
        end_date = (
            datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=UTC)
            if args.date
            else datetime.now(UTC) - timedelta(days=1)
        )
        days = args.days

    return [(end_date - timedelta(days=i)).strftime("%Y-%m-%d") for i in reversed(range(days))]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dump Matomo metrics as a SQL file (no PostgreSQL connection needed)"
    )
    parser.add_argument("--date", help="Most recent date to dump (YYYY-MM-DD). Default: yesterday")
    parser.add_argument(
        "--days", type=int, default=1, help="Number of days to dump (going back from date)"
    )
    parser.add_argument(
        "--all", action="store_true", help="Dump all data from Matomo since its creation"
    )
    parser.add_argument(
        "-o", "--output", help="Dump file path. Default: <dump-dir>/matomo-metrics-<dates>.sql"
    )
    parser.add_argument(
        "--dump-dir", default=str(DUMP_DIR), help=f"Directory for dumps (default: {DUMP_DIR})"
    )
    parser.add_argument(
        "--no-schema",
        dest="with_schema",
        action="store_false",
        help="Omit CREATE SCHEMA/TABLE/INDEX statements (data only)",
    )
    parser.add_argument(
        "--no-refresh",
        dest="with_refresh",
        action="store_false",
        help="Omit the REFRESH MATERIALIZED VIEW block",
    )
    parser.add_argument(
        "--import",
        dest="do_import",
        action="store_true",
        help="Legacy: write into PostgreSQL instead of producing a dump file",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dates = resolve_dates(args)

    print("Building slug -> ObjectId lookup from MongoDB...")
    lookup = build_slug_to_oid_lookup()
    print(f"  {len(lookup)} entries in lookup table")

    if args.do_import:
        import_to_postgres(dates, lookup)
    else:
        dump_to_sql(
            dates,
            lookup,
            output=args.output,
            dump_dir=Path(args.dump_dir),
            with_schema=args.with_schema,
            with_refresh=args.with_refresh,
        )

    print("\nDone!")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
