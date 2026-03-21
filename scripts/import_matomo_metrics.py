#!/usr/bin/env python3
"""
Import metrics from Matomo (Piwik) into the Hydra CSV PostgreSQL database.

Fetches page view and outlink data from the Matomo API at dados.gov.pt/stats/
and inserts it into the metric schema tables (visits_datasets, visits_resources,
visits_reuses, visits_organizations, visits_dataservices, matomo_datasets, etc.).

Based on: https://github.com/datagouv/datagouvfr_data_pipelines/tree/main/dgv/metrics

Usage:
    python3 import_matomo_metrics.py [--date 2026-03-20] [--period day]
    docker exec airflow-demo-test python3 /tmp/import_matomo_metrics.py
"""

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import psycopg2
import requests
from pymongo import MongoClient

# Matomo config (from backend .env)
MATOMO_URL = "https://dados.gov.pt/stats/index.php"
MATOMO_TOKEN = "2a62abafa550d3aaba8c7a6a4bd1769b"
MATOMO_SITE_ID = 3

# MongoDB config (for slug -> ObjectId resolution)
MONGO_HOST = "10.55.37.40"
MONGO_PORT = 27017
MONGO_DB = "udata"

# PostgreSQL config (hydra_postgres_csv)
PG_HOST = "127.0.0.1"
PG_PORT = 5434
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DB = "postgres"

# URL patterns to extract object type and ID
PATTERNS = {
    "datasets": re.compile(r"/(?:pt|en|fr|es)/datasets/([a-z0-9][a-z0-9-]*[a-z0-9])(?:/|$)"),
    "reuses": re.compile(r"/(?:pt|en|fr|es)/reuses/([a-z0-9][a-z0-9-]*[a-z0-9])(?:/|$)"),
    "organizations": re.compile(r"/(?:pt|en|fr|es)/organizations/([a-z0-9][a-z0-9-]*[a-z0-9])(?:/|$)"),
    "dataservices": re.compile(r"/(?:pt|en|fr|es)/dataservices/([a-z0-9][a-z0-9-]*[a-z0-9])(?:/|$)"),
    "resources": re.compile(r"/(?:pt|en|fr|es)/datasets/r/([a-f0-9-]{36}|[a-f0-9]{24})(?:/|$)"),
}


def matomo_api(method, date, **extra_params):
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


def build_slug_to_oid_lookup():
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


def extract_visits_from_matomo(date):
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


def extract_outlinks_from_matomo(date):
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


def save_visits_to_postgres(conn, visits, date, lookup):
    """Insert visit metrics into PostgreSQL."""
    cur = conn.cursor()

    table_map = {
        "datasets": ("metric.visits_datasets", "dataset_id"),
        "reuses": ("metric.visits_reuses", "reuse_id"),
        "organizations": ("metric.visits_organizations", "organization_id"),
        "dataservices": ("metric.visits_dataservices", "dataservice_id"),
        "resources": ("metric.visits_resources", "resource_id"),
    }

    total_inserted = 0

    for obj_type, slugs in visits.items():
        table, id_col = table_map.get(obj_type, (None, None))
        if not table:
            continue

        for slug, nb_visit in slugs.items():
            info = lookup.get((obj_type, slug))
            if not info:
                continue

            obj_id = info["id"]
            org_id = info.get("organization_id")

            if obj_type == "resources":
                dataset_id = info.get("dataset_id")
                cur.execute(
                    f"""
                    INSERT INTO {table} (date_metric, {id_col}, dataset_id, organization_id, nb_visit)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT ({id_col}, date_metric)
                    DO UPDATE SET nb_visit = EXCLUDED.nb_visit
                    """,
                    (date, obj_id, dataset_id, org_id, nb_visit),
                )
            elif obj_type == "organizations":
                cur.execute(
                    f"""
                    INSERT INTO {table} (date_metric, {id_col}, nb_visit)
                    VALUES (%s, %s, %s)
                    ON CONFLICT ({id_col}, date_metric)
                    DO UPDATE SET nb_visit = EXCLUDED.nb_visit
                    """,
                    (date, obj_id, nb_visit),
                )
            else:
                cur.execute(
                    f"""
                    INSERT INTO {table} (date_metric, {id_col}, organization_id, nb_visit)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT ({id_col}, date_metric)
                    DO UPDATE SET nb_visit = EXCLUDED.nb_visit
                    """,
                    (date, obj_id, org_id, nb_visit),
                )
            total_inserted += 1

    conn.commit()
    cur.close()
    print(f"  Inserted/updated {total_inserted} visit records")
    return total_inserted


def refresh_materialized_views(conn):
    """Refresh all materialized views in the correct order."""
    print("  Refreshing materialized views...")
    cur = conn.cursor()
    views = [
        "metric.metrics_datasets",
        "metric.metrics_reuses",
        "metric.metrics_dataservices",
        "metric.metrics_organizations",
        "metric.datasets",
        "metric.datasets_total",
        "metric.resources",
        "metric.resources_total",
        "metric.organizations",
        "metric.organizations_total",
        "metric.reuses",
        "metric.reuses_total",
        "metric.dataservices",
        "metric.dataservices_total",
        "metric.site",
    ]
    for view in views:
        cur.execute(f"REFRESH MATERIALIZED VIEW {view}")
    conn.commit()
    cur.close()
    print(f"  Refreshed {len(views)} materialized views")


def ensure_upsert_indexes(conn):
    """Create unique indexes needed for upsert operations."""
    cur = conn.cursor()
    indexes = [
        ("visits_datasets_upsert_idx", "metric.visits_datasets", "(dataset_id, date_metric)"),
        ("visits_resources_upsert_idx", "metric.visits_resources", "(resource_id, date_metric)"),
        ("visits_reuses_upsert_idx", "metric.visits_reuses", "(reuse_id, date_metric)"),
        ("visits_organizations_upsert_idx", "metric.visits_organizations", "(organization_id, date_metric)"),
        ("visits_dataservices_upsert_idx", "metric.visits_dataservices", "(dataservice_id, date_metric)"),
    ]
    for name, table, cols in indexes:
        cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {name} ON {table} {cols}")
    conn.commit()
    cur.close()


def main():
    parser = argparse.ArgumentParser(description="Import Matomo metrics into PostgreSQL")
    parser.add_argument("--date", help="Date to import (YYYY-MM-DD). Default: yesterday")
    parser.add_argument("--days", type=int, default=1, help="Number of days to import (going back from date)")
    parser.add_argument("--all", action="store_true", help="Import all data from Matomo since its creation")
    args = parser.parse_args()

    if args.all:
        start_date = datetime.utcnow() - timedelta(days=1)
        # Matomo site created on 2018-07-19
        matomo_start = datetime(2018, 7, 19)
        args.days = (start_date - matomo_start).days
        print(f"Importing ALL data: {args.days} days (from {matomo_start.date()} to {start_date.date()})")
    elif args.date:
        start_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        start_date = datetime.utcnow() - timedelta(days=1)

    print(f"Building slug -> ObjectId lookup from MongoDB...")
    lookup = build_slug_to_oid_lookup()
    print(f"  {len(lookup)} entries in lookup table")

    print(f"Connecting to PostgreSQL {PG_HOST}:{PG_PORT}...")
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB)

    ensure_upsert_indexes(conn)

    for i in range(args.days):
        date = (start_date - timedelta(days=i)).strftime("%Y-%m-%d")
        print(f"\n=== Processing {date} ===")

        visits = extract_visits_from_matomo(date)
        save_visits_to_postgres(conn, visits, date, lookup)

    refresh_materialized_views(conn)

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
