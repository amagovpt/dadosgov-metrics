from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

UDATA_MONGO_DB = "udata"
METRICS_PG_CONN_ID = "hydra_postgres_csv"
METRICS_MONGO_DB = "etl_logs"
try:
    from airflow.models import Variable
    METRICS_API_URL = Variable.get("METRICS_API_URL", default_var="http://host.docker.internal:8006/api")
except Exception:
    METRICS_API_URL = "http://host.docker.internal:8006/api"

# Janela, em dias, das agregações DIÁRIAS (as que alimentam
# metric.visits_datasets e metric.visits_resources). Só essas: os totais
# cumulativos mais abaixo têm de continuar a varrer o histórico completo, senão
# passavam a ser "totais dos últimos N dias" e o MongoDB/udata recebia valores
# errados.
#
# Porque é seguro janelar as diárias: os destinos fazem UPSERT com índice único
# em (dataset_id, date_metric) / (resource_id, date_metric), pelo que reprocessar
# só a janela recente é equivalente a reprocessar tudo — as linhas mais antigas
# ficam como estão. E ficam: a 2026-09-17 o metric.visits_datasets tinha 962 764
# linhas desde 2018-07-24, enquanto o metric_event no Mongo tem TTL de 90 dias
# (índice created_at_1, expireAfterSeconds=7776000). O Mongo é a store de curto
# prazo, o Postgres é o armazém.
#
# 14 dias dá margem para eventos que cheguem atrasados e não custa nada: a
# atividade recente é pouca, por isso 7 e 14 dias dão o mesmo payload (7,2 MB).
# Sem a janela eram 147,3 MB por run, 96 runs/dia — 95% do payload era o mesmo
# bloco histórico re-enviado de cada vez.
#
# Para um backfill completo (ambiente novo, ou recuperar uma falha longa), pôr a
# Variable METRICS_DAILY_WINDOW_DAYS a 0 corre sem filtro, como antes.
try:
    from airflow.models import Variable as _Variable
    _window_raw = _Variable.get("METRICS_DAILY_WINDOW_DAYS", default_var="14")
except Exception:
    _window_raw = "14"
try:
    DAILY_WINDOW_DAYS = int(_window_raw)
except ValueError:
    # Um valor inválido ("off", "0 dias") caía em silêncio para 14 — e quem
    # tentasse desligar a janela para um backfill não dava por isso.
    logger.warning(
        "METRICS_DAILY_WINDOW_DAYS=%r não é inteiro; a usar 14 dias", _window_raw
    )
    DAILY_WINDOW_DAYS = 14


def _daily_window_match():
    """Cláusulas de '$match' da janela, ou {} se estiver desligada.

    O corte é à MEIA-NOITE UTC de (hoje - DAILY_WINDOW_DAYS), nunca 'agora - N
    dias' com hora. As diárias agrupam por dia ('$dateToString', em UTC) e o
    destino faz 'DO UPDATE SET nb_visit = EXCLUDED.nb_visit' — substitui, não
    soma nem faz max. Com um corte a meio do dia, o dia da fronteira entrava só
    parcialmente (os eventos depois da hora do corte) e a cada run de 15 min o
    seu nb_visit era reescrito com um valor cada vez menor, até sobrar só o dos
    últimos minutos do dia: um dia de histórico corrompido por dia, em silêncio.
    Alinhado à meia-noite, cada dia está inteiro dentro da janela ou inteiro
    fora — nunca parcial. (Achado da auditoria de 2026-09-17; estava latente
    porque a fronteira caía no intervalo sem eventos de 08-25..09-08.)
    """
    if DAILY_WINDOW_DAYS <= 0:
        return {}
    from datetime import timezone

    hoje = datetime.now(timezone.utc).date()
    corte = datetime.combine(
        hoje - timedelta(days=DAILY_WINDOW_DAYS),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    return {"created_at": {"$gte": corte}}


def _id_or_slug_query(identifier):
    """Return a MongoDB query matching by ObjectId or slug."""
    from bson import ObjectId
    try:
        return {"_id": ObjectId(identifier)}
    except Exception:
        return {"slug": identifier}


def extract_tracking_events():
    """Aggregate views and downloads from tracking_events collection."""
    from airflow.providers.mongo.hooks.mongo import MongoHook

    hook = MongoHook(conn_id="mongo_default")
    client = hook.get_conn()
    db = client[UDATA_MONGO_DB]

    # Build slug -> ObjectId lookup and dataset_id -> organization_id lookup
    slug_to_oid = {}
    org_lookup = {}
    for doc in db["dataset"].find(
        {"deleted": None},
        {"slug": 1, "organization": 1},
    ):
        oid = str(doc["_id"])
        slug_to_oid[doc.get("slug", "")] = oid
        if doc.get("organization"):
            org_lookup[oid] = str(doc["organization"])

    def resolve_dataset_id(identifier):
        """Resolve slug or ObjectId string to a canonical ObjectId string."""
        if not identifier:
            return None
        return slug_to_oid.get(identifier, identifier)

    # Build resource_id -> dataset_id lookup
    resource_dataset_lookup = {}
    for doc in db["dataset"].find(
        {"resources": {"$exists": True}, "deleted": None},
        {"resources._id": 1},
    ):
        for res in doc.get("resources", []):
            resource_dataset_lookup[str(res["_id"])] = str(doc["_id"])

    # Daily views per dataset (for PostgreSQL metric.visits_datasets)
    dataset_views_daily = []
    for doc in db["metric_event"].aggregate(
        [
            {
                "$match": {
                    "object_type": "dataset",
                    "event_type": "view",
                    **_daily_window_match(),
                }
            },
            {
                "$group": {
                    "_id": {
                        "object_id": "$object_id",
                        "date": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$created_at",
                            }
                        },
                    },
                    "total": {"$sum": 1},
                }
            },
        ]
    ):
        dataset_id = resolve_dataset_id(doc["_id"]["object_id"])
        if not dataset_id:
            continue
        dataset_views_daily.append(
            {
                "dataset_id": dataset_id,
                "date_metric": doc["_id"]["date"],
                "nb_visit": doc["total"],
                "organization_id": org_lookup.get(dataset_id),
            }
        )

    # Daily downloads per resource (for PostgreSQL metric.visits_resources)
    resource_downloads_daily = []
    for doc in db["metric_event"].aggregate(
        [
            {
                "$match": {
                    "event_type": "download",
                    "extra.resource_id": {"$exists": True, "$ne": None},
                    **_daily_window_match(),
                }
            },
            {
                "$group": {
                    "_id": {
                        "resource_id": "$extra.resource_id",
                        "date": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$created_at",
                            }
                        },
                    },
                    "total": {"$sum": 1},
                }
            },
        ]
    ):
        resource_id = doc["_id"]["resource_id"]
        dataset_id = resource_dataset_lookup.get(resource_id)
        resource_downloads_daily.append(
            {
                "resource_id": resource_id,
                "dataset_id": dataset_id,
                "date_metric": doc["_id"]["date"],
                "nb_visit": doc["total"],
                "organization_id": (
                    org_lookup.get(dataset_id) if dataset_id else None
                ),
            }
        )

    # Total views per dataset (for MongoDB updates)
    view_counts = {}
    for doc in db["metric_event"].aggregate(
        [
            {"$match": {"object_type": "dataset", "event_type": "view"}},
            {"$group": {"_id": "$object_id", "total": {"$sum": 1}}},
        ]
    ):
        did = resolve_dataset_id(doc["_id"])
        if did:
            view_counts[did] = view_counts.get(did, 0) + doc["total"]

    # Total downloads per dataset (for MongoDB updates)
    download_counts = {}
    for doc in db["metric_event"].aggregate(
        [
            {"$match": {"object_type": "dataset", "event_type": "download"}},
            {"$group": {"_id": "$object_id", "total": {"$sum": 1}}},
        ]
    ):
        did = resolve_dataset_id(doc["_id"])
        if did:
            download_counts[did] = download_counts.get(did, 0) + doc["total"]

    # Downloads per individual resource
    resource_downloads = {}
    for doc in db["metric_event"].aggregate(
        [
            {
                "$match": {
                    "event_type": "download",
                    "extra.resource_id": {"$exists": True, "$ne": None},
                }
            },
            {"$group": {"_id": "$extra.resource_id", "total": {"$sum": 1}}},
        ]
    ):
        resource_downloads[doc["_id"]] = doc["total"]

    # Views per organization
    org_views = {
        doc["_id"]: doc["total"]
        for doc in db["metric_event"].aggregate(
            [
                {"$match": {"object_type": "organization", "event_type": "view"}},
                {"$group": {"_id": "$object_id", "total": {"$sum": 1}}},
            ]
        )
    }

    # Views per reuse
    reuse_views = {
        doc["_id"]: doc["total"]
        for doc in db["metric_event"].aggregate(
            [
                {"$match": {"object_type": "reuse", "event_type": "view"}},
                {"$group": {"_id": "$object_id", "total": {"$sum": 1}}},
            ]
        )
    }

    # Views per dataservice
    dataservice_views = {
        doc["_id"]: doc["total"]
        for doc in db["metric_event"].aggregate(
            [
                {"$match": {"object_type": "dataservice", "event_type": "view"}},
                {"$group": {"_id": "$object_id", "total": {"$sum": 1}}},
            ]
        )
    }

    # Site-level statistics
    site_counts = {
        "datasets": db["dataset"].count_documents({"deleted": None}),
        "resources": db["dataset"]
        .aggregate(
            [
                {"$match": {"deleted": None}},
                {"$project": {"n": {"$size": {"$ifNull": ["$resources", []]}}}},
                {"$group": {"_id": None, "total": {"$sum": "$n"}}},
            ]
        )
        .next()
        .get("total", 0),
        "organizations": db["organization"].count_documents({"deleted": None}),
        "reuses": db["reuse"].count_documents({"deleted": None}),
        "users": db["user"].estimated_document_count(),
        "discussions": db["discussion"].estimated_document_count(),
        "dataservices": (
            db["dataservice"].count_documents({"deleted": None})
            if "dataservice" in db.list_collection_names()
            else 0
        ),
        "followers": (
            db["follow"].estimated_document_count()
            if "follow" in db.list_collection_names()
            else 0
        ),
    }

    total_events = db["metric_event"].count_documents({})
    client.close()

    logger.info(
        "Aggregated %d events (janela das diarias: %s): %d dataset views, %d downloads, "
        "%d resource downloads, %d org views, %d reuse views, %d daily dataset rows, "
        "%d daily resource rows",
        total_events,
        ("%d dias" % DAILY_WINDOW_DAYS) if DAILY_WINDOW_DAYS > 0 else "desligada (historico completo)",
        len(view_counts),
        len(download_counts),
        len(resource_downloads),
        len(org_views),
        len(reuse_views),
        len(dataset_views_daily),
        len(resource_downloads_daily),
    )
    return {
        "dataset_views": view_counts,
        "dataset_downloads": download_counts,
        "resource_downloads": resource_downloads,
        "org_views": org_views,
        "reuse_views": reuse_views,
        "dataservice_views": dataservice_views,
        "site_counts": site_counts,
        "total_events": total_events,
        "dataset_views_daily": dataset_views_daily,
        "resource_downloads_daily": resource_downloads_daily,
    }


def send_to_metrics_db(ti):
    """Write daily views/downloads to PostgreSQL metric schema (base tables)."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    extracted = ti.xcom_pull(task_ids="extract_tracking_events")
    if not extracted:
        raise ValueError("No data received")

    hook = PostgresHook(postgres_conn_id=METRICS_PG_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Ensure unique indexes for upsert
    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS visits_datasets_upsert_idx
        ON metric.visits_datasets (dataset_id, date_metric)
        """
    )
    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS visits_resources_upsert_idx
        ON metric.visits_resources (resource_id, date_metric)
        """
    )
    conn.commit()

    # Write dataset views to metric.visits_datasets
    datasets_inserted = 0
    for row in extracted.get("dataset_views_daily", []):
        cursor.execute(
            """
            INSERT INTO metric.visits_datasets
                (date_metric, dataset_id, organization_id, nb_visit)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (dataset_id, date_metric)
            DO UPDATE SET nb_visit = EXCLUDED.nb_visit,
                          organization_id = COALESCE(EXCLUDED.organization_id,
                                                     metric.visits_datasets.organization_id)
            """,
            (
                row["date_metric"],
                row["dataset_id"],
                row.get("organization_id"),
                row["nb_visit"],
            ),
        )
        datasets_inserted += 1

    # Write resource downloads to metric.visits_resources
    resources_inserted = 0
    for row in extracted.get("resource_downloads_daily", []):
        cursor.execute(
            """
            INSERT INTO metric.visits_resources
                (date_metric, resource_id, dataset_id, organization_id, nb_visit)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (resource_id, date_metric)
            DO UPDATE SET nb_visit = EXCLUDED.nb_visit,
                          dataset_id = COALESCE(EXCLUDED.dataset_id,
                                                metric.visits_resources.dataset_id),
                          organization_id = COALESCE(EXCLUDED.organization_id,
                                                     metric.visits_resources.organization_id)
            """,
            (
                row["date_metric"],
                row["resource_id"],
                row.get("dataset_id"),
                row.get("organization_id"),
                row["nb_visit"],
            ),
        )
        resources_inserted += 1

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(
        "Wrote %d dataset visit rows + %d resource download rows to PostgreSQL metric schema",
        datasets_inserted,
        resources_inserted,
    )
    return {
        "datasets_inserted": datasets_inserted,
        "resources_inserted": resources_inserted,
    }


def refresh_materialized_views():
    """Refresh all materialized views so PostgREST serves fresh data."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=METRICS_PG_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Order matters: base views first, then aggregated views that depend on them
    views = [
        "metric.metrics_datasets",
        "metric.metrics_organizations",
        "metric.metrics_reuses",
        "metric.metrics_dataservices",
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

    refreshed = 0
    for view in views:
        cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")
        refreshed += 1
        logger.info("Refreshed %s", view)

    conn.commit()
    cursor.close()
    conn.close()

    logger.info("Refreshed %d materialized views", refreshed)
    return {"refreshed": refreshed}


def _get_with_retry(url, timeout=30, max_retries=3):
    """GET url with retry on ChunkedEncodingError / connection errors."""
    import time
    import requests

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError) as exc:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt
            logger.warning("Request failed (%s), retrying in %ds (attempt %d/%d)",
                           exc, wait, attempt + 1, max_retries)
            time.sleep(wait)


def _update_views_from_totals(db, collection, endpoint, id_field, max_pages=10000):
    """Write metrics.views from a *_total metrics endpoint into a udata collection.

    Same pattern as the datasets loop: the metrics API aggregates
    metric.visits_<objects>, which is where the Matomo history lives.
    Returns how many documents were updated.
    """
    updated = 0
    page = 1
    while page <= max_pages:
        url = f"{METRICS_API_URL}/{endpoint}/data/?visit__greater=0&page_size=50&page={page}"
        data = _get_with_retry(url)

        for row in data["data"]:
            object_id = row.get(id_field)
            visit = row.get("visit") or 0
            if not object_id:
                continue
            db[collection].update_one(
                _id_or_slug_query(object_id),
                {"$set": {"metrics.views": visit}},
            )
            updated += 1

        if not data["links"].get("next"):
            break
        page += 1

    logger.info("Updated metrics.views for %d %s(s) from %s", updated, collection, endpoint)
    return updated


def update_udata_metrics(ti):
    """Read totals from datasets_total (PostgREST) and write to udata MongoDB.
    Also computes per-object statistics and per-resource download counts."""
    from airflow.providers.mongo.hooks.mongo import MongoHook

    extracted = ti.xcom_pull(task_ids="extract_tracking_events")
    if not extracted:
        raise ValueError("No data received")

    hook = MongoHook(conn_id="mongo_default")
    client = hook.get_conn()
    db = client[UDATA_MONGO_DB]
    updated = 0
    page = 1
    max_pages = 10000

    # 1. Update views + downloads from PostgreSQL (datasets_total)
    while page <= max_pages:
        url = f"{METRICS_API_URL}/datasets_total/data/?visit__greater=0&page_size=50&page={page}"
        data = _get_with_retry(url)

        for row in data["data"]:
            dataset_id = row.get("dataset_id")
            visit = row.get("visit") or 0
            download_resource = row.get("download_resource") or 0
            if not dataset_id:
                continue
            db["dataset"].update_one(
                _id_or_slug_query(dataset_id),
                {
                    "$set": {
                        "metrics.views": visit,
                        "metrics.resources_downloads": download_resource,
                    }
                },
            )
            updated += 1

        if not data["links"].get("next"):
            break
        page += 1

    # 2. Update per-resource download counts (resource.metrics.views)
    resource_downloads = extracted.get("resource_downloads", {})
    resource_updated = 0
    for resource_id, count in resource_downloads.items():
        result = db["dataset"].update_one(
            {"resources._id": resource_id},
            {"$set": {"resources.$.metrics.views": count}},
        )
        if result.modified_count > 0:
            resource_updated += 1

    logger.info("Updated download counts for %d individual resources", resource_updated)

    # 3. Compute per-dataset statistics: followers, reuses, discussions
    for doc in db["follow"].aggregate(
        [
            {"$match": {"following.collection": "dataset"}},
            {"$group": {"_id": "$following.id", "count": {"$sum": 1}}},
        ]
    ):
        db["dataset"].update_one(
            {"_id": doc["_id"]},
            {"$set": {"metrics.followers": doc["count"]}},
        )

    for doc in db["discussion"].aggregate(
        [
            {
                "$group": {
                    "_id": "$subject",
                    "total": {"$sum": 1},
                    "open": {"$sum": {"$cond": [{"$eq": ["$closed", None]}, 1, 0]}},
                }
            },
        ]
    ):
        db["dataset"].update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "metrics.discussions": doc["total"],
                    "metrics.discussions_open": doc["open"],
                }
            },
        )

    for doc in db["reuse"].aggregate(
        [
            {"$match": {"deleted": None}},
            {"$unwind": "$datasets"},
            {"$group": {"_id": "$datasets", "count": {"$sum": 1}}},
        ]
    ):
        db["dataset"].update_one(
            {"_id": doc["_id"]},
            {"$set": {"metrics.reuses": doc["count"]}},
        )

    # 4. Compute per-organization statistics
    for doc in db["dataset"].aggregate(
        [
            {"$match": {"deleted": None, "organization": {"$ne": None}}},
            {"$group": {"_id": "$organization", "count": {"$sum": 1}}},
        ]
    ):
        db["organization"].update_one(
            {"_id": doc["_id"]},
            {"$set": {"metrics.datasets": doc["count"]}},
        )

    for doc in db["reuse"].aggregate(
        [
            {"$match": {"deleted": None, "organization": {"$ne": None}}},
            {"$group": {"_id": "$organization", "count": {"$sum": 1}}},
        ]
    ):
        db["organization"].update_one(
            {"_id": doc["_id"]},
            {"$set": {"metrics.reuses": doc["count"]}},
        )

    for doc in db["follow"].aggregate(
        [
            {"$match": {"following.collection": "organization"}},
            {"$group": {"_id": "$following.id", "count": {"$sum": 1}}},
        ]
    ):
        db["organization"].update_one(
            {"_id": doc["_id"]},
            {"$set": {"metrics.followers": doc["count"]}},
        )

    # Organization and reuse page views come from PostgreSQL (same source as
    # datasets, via organizations_total / reuses_total), not from the tracking
    # events: metric_event only holds what udata itself recorded, which misses
    # the whole Matomo history imported into metric.visits_*.
    org_updated = _update_views_from_totals(
        db, "organization", "organizations_total", "organization_id", max_pages
    )
    reuse_updated = _update_views_from_totals(
        db, "reuse", "reuses_total", "reuse_id", max_pages
    )

    for ds_id, views in extracted.get("dataservice_views", {}).items():
        db["dataservice"].update_one(
            _id_or_slug_query(ds_id),
            {"$set": {"metrics.views": views}},
        )

    # 5. Update site-level metrics
    site_counts = extracted["site_counts"]
    today = datetime.utcnow().strftime("%Y-%m-%d")

    db["site"].update_one(
        {"_id": "dados.gov.pt"},
        {"$set": {f"metrics.{k}": v for k, v in site_counts.items()}},
    )

    db["metrics"].update_one(
        {"object_id": "dados.gov.pt", "date": today, "level": "daily"},
        {"$set": {"values": site_counts}},
        upsert=True,
    )

    client.close()
    logger.info(
        "Updated %d datasets + %d resources + %d organizations + %d reuses "
        "+ site stats in MongoDB for %s",
        updated,
        resource_updated,
        org_updated,
        reuse_updated,
        today,
    )
    return {
        "datasets_updated": updated,
        "resources_updated": resource_updated,
        "organizations_updated": org_updated,
        "reuses_updated": reuse_updated,
        "site_metrics_date": today,
    }


def save_to_mongodb(ti):
    """Save ETL log."""
    from airflow.providers.mongo.hooks.mongo import MongoHook

    extracted = ti.xcom_pull(task_ids="extract_tracking_events")
    pg_result = ti.xcom_pull(task_ids="send_to_metrics_db")
    udata_result = ti.xcom_pull(task_ids="update_udata_metrics")

    log_doc = {
        "timestamp": datetime.utcnow().isoformat(),
        "dag_id": "metrics_etl",
        "total_tracking_events": extracted.get("total_events", 0) if extracted else 0,
        "datasets_with_views": (
            len(extracted.get("dataset_views", {})) if extracted else 0
        ),
        "datasets_with_downloads": (
            len(extracted.get("dataset_downloads", {})) if extracted else 0
        ),
        "resources_with_downloads": (
            len(extracted.get("resource_downloads", {})) if extracted else 0
        ),
        "metrics_written_pg": pg_result.get("datasets_inserted", 0) if pg_result else 0,
        "resources_written_pg": (
            pg_result.get("resources_inserted", 0) if pg_result else 0
        ),
        "datasets_updated_mongo": (
            udata_result.get("datasets_updated", 0) if udata_result else 0
        ),
        "resources_updated_mongo": (
            udata_result.get("resources_updated", 0) if udata_result else 0
        ),
        "site_counts": extracted.get("site_counts") if extracted else {},
        "status": "success",
    }

    hook = MongoHook(conn_id="mongo_default")
    hook.insert_one(
        mongo_collection="metrics_logs", doc=log_doc, mongo_db=METRICS_MONGO_DB
    )
    logger.info("ETL log saved")


with DAG(
    dag_id="metrics_etl",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["metrics"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
) as dag:
    extrair = PythonOperator(
        task_id="extract_tracking_events",
        python_callable=extract_tracking_events,
        # O retorno desta task é o histórico completo do metric_event (~148 MB
        # em 2026-09 e a crescer, porque nenhuma agregação filtra por data).
        # Sem este flag o PythonOperator escreve-o inteiro no log da task, numa
        # única linha 'Done. Returned value was: ...' — com 96 runs/dia eram
        # ~14 GB/dia a entrar em ./logs.
        # O valor continua a ser enviado para o XCom: send_to_metrics_db,
        # refresh_materialized_views e update_udata_metrics leem-no de lá.
        show_return_value_in_logs=False,
    )
    enviar_pg = PythonOperator(
        task_id="send_to_metrics_db", python_callable=send_to_metrics_db
    )
    refrescar_views = PythonOperator(
        task_id="refresh_materialized_views",
        python_callable=refresh_materialized_views,
    )
    atualizar_udata = PythonOperator(
        task_id="update_udata_metrics", python_callable=update_udata_metrics
    )
    registar_log = PythonOperator(
        task_id="save_to_mongodb", python_callable=save_to_mongodb
    )

    extrair >> enviar_pg >> refrescar_views >> atualizar_udata >> registar_log
