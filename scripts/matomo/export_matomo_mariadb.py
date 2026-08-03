#!/usr/bin/env python3
"""
Exporta as metricas do Matomo directamente da base de dados MariaDB.

Corre NA VM DO MATOMO. Substitui a extracao via Reporting API HTTP que
import_matomo_metrics.py faz (o URL https://dados.gov.pt/stats/ deixou de estar
disponivel na intranet). Le a MariaDB em modo READ-ONLY e escreve ficheiros
NDJSON comprimidos + manifesto, prontos para copiar (scp) para a VM do MongoDB e
carregar com import_matomo_export_to_mongo.py.

Metricas extraidas, por dia e no fuso horario do site:
  - pageviews   (log_action.type = 1)   -> nb_visits / nb_hits por objecto udata
  - outlinks    (type = 2)              -> cliques em links externos
  - downloads   (type = 3)              -> transferencias de ficheiros
  - site search (type = 8)              -> pesquisas internas
  - eventos     (type = 10 / 11 / 12)   -> categoria / acao / nome

Uso:
    # 1) Diagnostico primeiro: que sites existem, que historico existe mesmo
    python3 export_matomo_mariadb.py --info --config-ini /var/www/matomo/config/config.ini.php

    # 2) Export de um dia (teste)
    python3 export_matomo_mariadb.py --site-id 3 --date-from 2026-07-30 --date-to 2026-07-30

    # 3) Export completo, empacotado para scp
    python3 export_matomo_mariadb.py --site-id 3 --all --verify-archive --tar

Requisitos: Python 3.9+ e um driver MySQL (pymysql recomendado: pip install pymysql).
"""

import argparse
import calendar
import configparser
import gzip
import hashlib
import json
import os
import re
import sys
import tarfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

SCRIPT_VERSION = "1.0.0"

# Tipos de acao do Matomo (Piwik\Tracker\Action::TYPE_*)
TYPE_PAGE_URL = 1
TYPE_OUTLINK = 2
TYPE_DOWNLOAD = 3
TYPE_SITE_SEARCH = 8
TYPE_EVENT_CATEGORY = 10
TYPE_EVENT_ACTION = 11
TYPE_EVENT_NAME = 12

# log_action.name e guardado sem o esquema; url_prefix diz qual era.
URL_PREFIXES = {0: "http://", 1: "http://www.", 2: "https://", 3: "https://www."}

# Padroes URL -> objecto udata. Copiados de import_matomo_metrics.py:57-63 para
# que os numeros sejam comparaveis com o que a API devolvia. "resources" e
# testado primeiro (resultado identico, mas deixa de depender da ordem do dict).
PATTERNS = [
    ("resources", re.compile(r"/(?:pt|en|fr|es)/datasets/r/([a-f0-9-]{36}|[a-f0-9]{24})(?:/|$)")),
    ("datasets", re.compile(r"/(?:pt|en|fr|es)/datasets/([a-z0-9][a-z0-9-]*[a-z0-9])(?:/|$)")),
    ("reuses", re.compile(r"/(?:pt|en|fr|es)/reuses/([a-z0-9][a-z0-9-]*[a-z0-9])(?:/|$)")),
    ("organizations", re.compile(r"/(?:pt|en|fr|es)/organizations/([a-z0-9][a-z0-9-]*[a-z0-9])(?:/|$)")),
    ("dataservices", re.compile(r"/(?:pt|en|fr|es)/dataservices/([a-z0-9][a-z0-9-]*[a-z0-9])(?:/|$)")),
]

# Prefiltro SQL para reduzir o volume transferido. So se aplica aos pageviews,
# onde as linhas que nao casam sao descartadas de qualquer forma; nos outlinks e
# downloads guardamos tudo (o alvo do clique interessa mesmo sem objecto udata).
PREFILTER_LIKE = ["%/datasets/%", "%/reuses/%", "%/organizations/%", "%/dataservices/%"]

RAW_FAMILIES = ["pageviews", "outlinks", "downloads", "searches", "events"]
AGG_FAMILIES = ["visits_daily", "outlinks_daily", "downloads_daily", "searches_daily", "events_daily"]


# --------------------------------------------------------------------------- #
# Ligacao a base de dados
# --------------------------------------------------------------------------- #

def load_config_ini(path):
    """Le a seccao [database] do config.ini.php do Matomo.

    O ficheiro comeca por "; <?php exit; ?> DO NOT REMOVE THIS LINE", que o
    configparser trata como comentario. Evita ter credenciais no codigo.
    """
    parser = configparser.RawConfigParser()
    parser.optionxform = str  # nao normalizar as chaves para minusculas
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        parser.read_file(fh)
    if not parser.has_section("database"):
        raise SystemExit("ERRO: %s nao tem seccao [database]" % path)

    def get(key, default=None):
        if parser.has_option("database", key):
            return parser.get("database", key).strip().strip('"').strip("'")
        return default

    host = get("host", "127.0.0.1")
    port = get("port", "3306")
    # O Matomo aceita "host = 127.0.0.1:3307" ou um socket unix.
    if host and ":" in host and not host.startswith("/"):
        host, _, maybe_port = host.rpartition(":")
        if maybe_port.isdigit():
            port = maybe_port
    return {
        "host": host,
        "port": int(port) if str(port).isdigit() else 3306,
        "user": get("username"),
        "password": get("password"),
        "database": get("dbname"),
        "prefix": get("tables_prefix", "matomo_"),
    }


def resolve_db_config(args):
    """Precedencia: CLI > variaveis de ambiente > config.ini.php."""
    cfg = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": None,
        "password": None,
        "database": "matomo",
        "prefix": "matomo_",
    }
    if args.config_ini:
        from_ini = load_config_ini(args.config_ini)
        cfg.update({k: v for k, v in from_ini.items() if v not in (None, "")})

    env_map = {
        "host": "MATOMO_DB_HOST",
        "port": "MATOMO_DB_PORT",
        "user": "MATOMO_DB_USER",
        "password": "MATOMO_DB_PASSWORD",
        "database": "MATOMO_DB_NAME",
        "prefix": "MATOMO_DB_PREFIX",
    }
    for key, env in env_map.items():
        val = os.environ.get(env)
        if val:
            cfg[key] = int(val) if key == "port" else val

    for key in ("host", "port", "user", "password", "database", "prefix"):
        val = getattr(args, "db_" + key, None)
        if val:
            cfg[key] = val

    if not cfg["user"]:
        raise SystemExit(
            "ERRO: utilizador da base de dados nao definido.\n"
            "  Use --config-ini /var/www/matomo/config/config.ini.php,\n"
            "  ou defina MATOMO_DB_USER / MATOMO_DB_PASSWORD,\n"
            "  ou passe --db-user / --db-password."
        )
    return cfg


def connect(cfg):
    """Liga a MariaDB com o primeiro driver disponivel.

    PyMySQL vem primeiro porque e puro-Python: instala-se offline por wheel, sem
    headers de sistema. Conta numa VM onde nem o cliente mysql existe.
    """
    attempts = []

    try:
        import pymysql

        return pymysql.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"] or "",
            database=cfg["database"],
            charset="utf8mb4",
            autocommit=False,
        ), "pymysql"
    except ImportError as exc:
        attempts.append("pymysql: %s" % exc)

    try:
        import MySQLdb

        return MySQLdb.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            passwd=cfg["password"] or "",
            db=cfg["database"],
            charset="utf8mb4",
        ), "mysqlclient"
    except ImportError as exc:
        attempts.append("mysqlclient: %s" % exc)

    try:
        import mysql.connector

        return mysql.connector.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"] or "",
            database=cfg["database"],
            charset="utf8mb4",
            autocommit=False,
        ), "mysql-connector"
    except ImportError as exc:
        attempts.append("mysql-connector: %s" % exc)

    raise SystemExit(
        "ERRO: nenhum driver MySQL/MariaDB encontrado.\n  "
        + "\n  ".join(attempts)
        + "\n\n  Instale um destes:  pip install pymysql   (recomendado, puro-Python)\n"
        "                      pip install mysqlclient\n"
        "                      pip install mysql-connector-python"
    )


def _dict_cursor(conn, driver, stream):
    """Cursor que devolve dicts; server-side (streaming) quando pedido.

    Streaming importa: log_link_visit_area pode ter milhoes de linhas por mes e
    um cursor normal carregaria o resultado todo em memoria.
    """
    if driver == "pymysql":
        import pymysql.cursors

        return conn.cursor(pymysql.cursors.SSDictCursor if stream else pymysql.cursors.DictCursor)
    if driver == "mysqlclient":
        import MySQLdb.cursors

        return conn.cursor(MySQLdb.cursors.SSDictCursor if stream else MySQLdb.cursors.DictCursor)
    # mysql-connector nao tem cursor server-side equivalente
    return conn.cursor(dictionary=True)


def query(conn, driver, sql, params=(), stream=False):
    """Executa uma consulta e devolve um iterador de dicts.

    Com stream=True so pode haver um iterador activo por conexao (limitacao dos
    cursores server-side) -- consumir sempre por inteiro antes da consulta seguinte.
    """
    cur = _dict_cursor(conn, driver, stream)
    cur.execute(sql, params)
    try:
        for row in cur:
            yield row
    finally:
        cur.close()


def fetch_all(conn, driver, sql, params=()):
    return list(query(conn, driver, sql, params))


def fetch_one(conn, driver, sql, params=()):
    rows = fetch_all(conn, driver, sql, params)
    return rows[0] if rows else None


# --------------------------------------------------------------------------- #
# Fuso horario / datas
# --------------------------------------------------------------------------- #

class DayBucketer:
    """Converte server_time (UTC) no dia local do site, como a API do Matomo faz.

    Nao usamos CONVERT_TZ porque exige as tabelas de fuso do MySQL carregadas, o
    que raramente esta feito.
    """

    def __init__(self, tz_name, force_utc=False):
        self.tz_name = tz_name
        self.tz = None
        self.mode = "utc"
        if force_utc or not tz_name:
            return
        try:
            from zoneinfo import ZoneInfo

            self.tz = ZoneInfo(tz_name)
            self.mode = "site-timezone"
        except Exception as exc:  # zoneinfo ausente ou sem tzdata do sistema
            print(
                "  AVISO: nao foi possivel usar o fuso '%s' (%s).\n"
                "         Os dias vao ser agrupados em UTC -- podem divergir da API do Matomo."
                % (tz_name, exc),
                file=sys.stderr,
            )

    def day(self, server_time):
        """server_time: datetime naive em UTC, como vem da MariaDB."""
        if self.tz is None:
            return server_time.date().isoformat()
        return server_time.replace(tzinfo=timezone.utc).astimezone(self.tz).date().isoformat()

    def utc_window(self, day_from, day_to):
        """Janela UTC que cobre [day_from, day_to] em hora local do site.

        Alarga um dia em cada extremo e filtra depois com precisao em Python --
        mais simples e correcto do que calcular offsets a lidar com mudancas de hora.
        """
        return (
            datetime.combine(day_from - timedelta(days=1), datetime.min.time()),
            datetime.combine(day_to + timedelta(days=2), datetime.min.time()),
        )


def month_chunks(day_from, day_to):
    """Parte o intervalo em blocos mensais [(chave, inicio, fim), ...].

    Cada dia cai exactamente num bloco, o que permite libertar os agregados no
    fim de cada mes sem risco de a mesma chave voltar a aparecer.
    """
    chunks = []
    cursor = day_from.replace(day=1)
    while cursor <= day_to:
        last_day = calendar.monthrange(cursor.year, cursor.month)[1]
        month_end = cursor.replace(day=last_day)
        chunks.append(
            ("%04d-%02d" % (cursor.year, cursor.month), max(cursor, day_from), min(month_end, day_to))
        )
        cursor = month_end + timedelta(days=1)
    return chunks


# --------------------------------------------------------------------------- #
# Mapeamento URL -> objecto udata
# --------------------------------------------------------------------------- #

def match_object(url_or_name):
    """Devolve (object_type, object_ref) ou (None, None)."""
    if not url_or_name:
        return None, None
    for obj_type, pattern in PATTERNS:
        found = pattern.search(url_or_name)
        if found:
            return obj_type, found.group(1)
    return None, None


def full_url(name, url_prefix):
    """Reconstroi o URL completo a partir de log_action.name + url_prefix."""
    if name is None:
        return None
    return URL_PREFIXES.get(url_prefix, "") + name


def natural_id(*parts):
    """_id determinista a partir da chave natural.

    Calculado aqui (e nao no importador) para que a importacao seja idempotente
    por construcao, sem duplicar a logica de chaves nos dois scripts.
    """
    joined = "\x1f".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def utc_iso(server_time):
    return server_time.replace(tzinfo=timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# Escrita NDJSON
# --------------------------------------------------------------------------- #

class NdjsonWriter:
    """Escreve NDJSON gzipado, uma linha por documento."""

    def __init__(self, path):
        self.path = path
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._fh = gzip.open(path, "wt", encoding="utf-8", compresslevel=6)
        self.count = 0

    def write(self, doc):
        self._fh.write(json.dumps(doc, ensure_ascii=False, separators=(",", ":")))
        self._fh.write("\n")
        self.count += 1

    def close(self):
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def count_ndjson_lines(path):
    total = 0
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for _ in fh:
            total += 1
    return total


# --------------------------------------------------------------------------- #
# Diagnostico (--info)
# --------------------------------------------------------------------------- #

def collect_coverage(conn, driver, prefix, site_id=None):
    """Mede o que existe mesmo na base de dados, antes de exportar."""
    info = {"generated_at": datetime.now(timezone.utc).isoformat(), "site_id": site_id}

    info["mariadb_version"] = fetch_one(conn, driver, "SELECT VERSION() AS v")["v"]

    row = fetch_one(
        conn,
        driver,
        "SELECT option_value FROM {p}option WHERE option_name = 'version_core'".format(p=prefix),
    )
    info["matomo_version"] = row["option_value"] if row else None

    info["sites"] = [
        {
            "idsite": int(r["idsite"]),
            "name": r["name"],
            "main_url": r["main_url"],
            "ts_created": r["ts_created"].isoformat() if r["ts_created"] else None,
            "timezone": r["timezone"],
        }
        for r in fetch_all(
            conn,
            driver,
            "SELECT idsite, name, main_url, ts_created, timezone FROM {p}site ORDER BY idsite".format(p=prefix),
        )
    ]

    # Purga de dados em bruto: se estiver activa, o historico antigo nao esta nas
    # tabelas de log e so existe nos arquivos agregados.
    info["purge_settings"] = {
        r["option_name"]: r["option_value"]
        for r in fetch_all(
            conn,
            driver,
            "SELECT option_name, option_value FROM {p}option "
            "WHERE option_name LIKE 'delete_logs%%' OR option_name LIKE 'delete_reports%%' "
            "ORDER BY option_name".format(p=prefix),
        )
    }
    info["raw_data_purge_enabled"] = str(info["purge_settings"].get("delete_logs_enable", "0")) == "1"

    # Cobertura dos logs em bruto: por site, para se poder confirmar qual e o idsite certo.
    per_site = {}
    for row in fetch_all(
        conn,
        driver,
        "SELECT idsite, MIN(server_time) AS min_t, MAX(server_time) AS max_t, COUNT(*) AS n, "
        "COUNT(DISTINCT idvisit) AS v FROM {p}log_link_visit_action GROUP BY idsite ORDER BY idsite".format(p=prefix),
    ):
        per_site[int(row["idsite"])] = {
            "min_server_time_utc": row["min_t"].isoformat() if row["min_t"] else None,
            "max_server_time_utc": row["max_t"].isoformat() if row["max_t"] else None,
            "total_actions": int(row["n"] or 0),
            "total_visits": int(row["v"] or 0),
        }
    info["raw_log_coverage_per_site"] = per_site
    info["raw_log_coverage"] = per_site.get(
        site_id, {"min_server_time_utc": None, "max_server_time_utc": None, "total_actions": 0, "total_visits": 0}
    )

    if site_id is not None:
        info["actions_per_month"] = [
            {"month": r["ym"], "actions": int(r["n"]), "visits": int(r["v"])}
            for r in fetch_all(
                conn,
                driver,
                "SELECT DATE_FORMAT(server_time, '%%Y-%%m') AS ym, COUNT(*) AS n, COUNT(DISTINCT idvisit) AS v "
                "FROM {p}log_link_visit_action WHERE idsite = %s GROUP BY ym ORDER BY ym".format(p=prefix),
                (site_id,),
            )
        ]
        info["actions_per_type"] = {
            str(r["type"]): int(r["n"])
            for r in fetch_all(
                conn,
                driver,
                "SELECT a.type AS type, COUNT(*) AS n FROM {p}log_link_visit_action llva "
                "JOIN {p}log_action a ON a.idaction = llva.idaction_url "
                "WHERE llva.idsite = %s GROUP BY a.type ORDER BY a.type".format(p=prefix),
                (site_id,),
            )
        }
    else:
        info["actions_per_month"] = []
        info["actions_per_type"] = {}

    archives = defaultdict(list)
    for row in fetch_all(
        conn,
        driver,
        "SELECT table_name AS t FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name LIKE %s ORDER BY table_name",
        (prefix + "archive%",),
    ):
        name = row["t"]
        kind = "numeric" if "_numeric_" in name else "blob" if "_blob_" in name else "other"
        archives[kind].append(name)
    info["archive_tables"] = dict(archives)

    return info


def print_coverage(info, pad="  "):
    print("%sMariaDB %s | Matomo %s" % (pad, info["mariadb_version"], info["matomo_version"]))
    print("%sSites:" % pad)
    for site in info["sites"]:
        cov = info["raw_log_coverage_per_site"].get(site["idsite"], {})
        marker = " <-- selecionado" if site["idsite"] == info.get("site_id") else ""
        print(
            "%s  idsite=%-3s %-26s %-32s tz=%-16s%s"
            % (pad, site["idsite"], site["name"][:26], (site["main_url"] or "")[:32], site["timezone"], marker)
        )
        print(
            "%s              logs: %s acoes de %s a %s"
            % (
                pad,
                cov.get("total_actions", 0),
                (cov.get("min_server_time_utc") or "-")[:10],
                (cov.get("max_server_time_utc") or "-")[:10],
            )
        )

    if info["raw_data_purge_enabled"]:
        print(
            "%sATENCAO: purga de dados em bruto ACTIVA -> %s" % (pad, info["purge_settings"])
        )
        print(
            "%s         O historico anterior ao limite de retencao so existe nos arquivos\n"
            "%s         agregados matomo_archive_*, que este script NAO le." % (pad, pad)
        )
    else:
        print("%sPurga de dados em bruto: desactivada ou nao configurada (bom sinal)." % pad)

    if info["actions_per_month"]:
        print("%sAcoes por mes (site %s):" % (pad, info["site_id"]))
        for row in info["actions_per_month"]:
            print("%s  %s  %9d acoes  %8d visitas" % (pad, row["month"], row["actions"], row["visits"]))
    if info["actions_per_type"]:
        print("%sAcoes por tipo (via idaction_url): %s" % (pad, info["actions_per_type"]))
        print("%s  1=pagina 2=outlink 3=download 4=titulo 8=pesquisa 10/11/12=evento" % pad)
    print(
        "%sTabelas de arquivo: %d numeric, %d blob"
        % (pad, len(info["archive_tables"].get("numeric", [])), len(info["archive_tables"].get("blob", [])))
    )


# --------------------------------------------------------------------------- #
# Extracao por familia
# --------------------------------------------------------------------------- #

def _accumulate(agg, key, idvisit):
    bucket = agg.get(key)
    if bucket is None:
        bucket = agg[key] = {"visits": set(), "hits": 0, "results": 0}
    bucket["visits"].add(idvisit)
    bucket["hits"] += 1
    return bucket


def extract_url_family(conn, driver, prefix, site_id, action_type, window, bucketer,
                       day_from, day_to, attribute_by_referrer, use_prefilter,
                       keep_unmatched, raw_writer):
    """Extrai pageviews (tipo 1), outlinks (2) ou downloads (3).

    attribute_by_referrer: no caso de outlinks e downloads o alvo do clique e
    externo, logo o objecto udata vem da pagina de origem (idaction_url_ref) --
    e o que o pipeline datagouv faz com segment=actionUrl==.../{model}/{slug}/.
    Cada documento leva sempre os dois: o URL de destino e o objecto de origem.
    """
    select_ref = ", r.name AS ref_name, r.url_prefix AS ref_url_prefix" if attribute_by_referrer else ""
    join_ref = (
        " LEFT JOIN {p}log_action r ON r.idaction = llva.idaction_url_ref" if attribute_by_referrer else ""
    )
    prefilter = ""
    params = [action_type, site_id, window[0], window[1]]
    if use_prefilter:
        prefilter = " AND (" + " OR ".join(["a.name LIKE %s"] * len(PREFILTER_LIKE)) + ")"
        params.extend(PREFILTER_LIKE)

    sql = (
        "SELECT llva.idlink_va, llva.idvisit, llva.server_time, llva.idpageview, llva.time_spent,"
        " a.name AS name, a.url_prefix AS url_prefix"
        + select_ref
        + " FROM {p}log_link_visit_action llva"
        " JOIN {p}log_action a ON a.idaction = llva.idaction_url AND a.type = %s"
        + join_ref
        + " WHERE llva.idsite = %s AND llva.server_time >= %s AND llva.server_time < %s"
        + prefilter
        + " ORDER BY llva.idlink_va"
    ).format(p=prefix)

    first, last = day_from.isoformat(), day_to.isoformat()
    agg = {}
    seen = 0
    for row in query(conn, driver, sql, tuple(params), stream=True):
        day = bucketer.day(row["server_time"])
        if day < first or day > last:
            continue  # margem da janela UTC
        seen += 1

        target = full_url(row["name"], row["url_prefix"])
        source = full_url(row.get("ref_name"), row.get("ref_url_prefix")) if attribute_by_referrer else target
        obj_type, obj_ref = match_object(source)
        if obj_type is None and not keep_unmatched:
            continue

        _accumulate(agg, (day, target if attribute_by_referrer else None, obj_type, obj_ref), row["idvisit"])

        if raw_writer is not None:
            raw_writer.write(
                {
                    "_id": int(row["idlink_va"]),
                    "site_id": site_id,
                    "idvisit": int(row["idvisit"]),
                    "server_time": utc_iso(row["server_time"]),
                    "date": day,
                    "idpageview": row["idpageview"],
                    "action_type": action_type,
                    "url": target,
                    "source_url": source if attribute_by_referrer else None,
                    "object_type": obj_type,
                    "object_ref": obj_ref,
                    "time_spent": int(row["time_spent"]) if row["time_spent"] is not None else None,
                }
            )
    return agg, seen


def extract_searches(conn, driver, prefix, site_id, window, bucketer, day_from, day_to, raw_writer):
    """Pesquisas internas: type = 8, ligado por idaction_name."""
    sql = (
        "SELECT llva.idlink_va, llva.idvisit, llva.server_time, llva.idpageview,"
        " llva.search_cat, llva.search_count, a.name AS keyword"
        " FROM {p}log_link_visit_action llva"
        " JOIN {p}log_action a ON a.idaction = llva.idaction_name AND a.type = %s"
        " WHERE llva.idsite = %s AND llva.server_time >= %s AND llva.server_time < %s"
        " ORDER BY llva.idlink_va"
    ).format(p=prefix)

    first, last = day_from.isoformat(), day_to.isoformat()
    agg = {}
    seen = 0
    for row in query(conn, driver, sql, (TYPE_SITE_SEARCH, site_id, window[0], window[1]), stream=True):
        day = bucketer.day(row["server_time"])
        if day < first or day > last:
            continue
        seen += 1
        bucket = _accumulate(agg, (day, row["keyword"], row["search_cat"]), row["idvisit"])
        if row["search_count"] is not None:
            bucket["results"] = int(row["search_count"])
        if raw_writer is not None:
            raw_writer.write(
                {
                    "_id": int(row["idlink_va"]),
                    "site_id": site_id,
                    "idvisit": int(row["idvisit"]),
                    "server_time": utc_iso(row["server_time"]),
                    "date": day,
                    "idpageview": row["idpageview"],
                    "action_type": TYPE_SITE_SEARCH,
                    "keyword": row["keyword"],
                    "search_cat": row["search_cat"],
                    "search_count": int(row["search_count"]) if row["search_count"] is not None else None,
                }
            )
    return agg, seen


def extract_events(conn, driver, prefix, site_id, window, bucketer, day_from, day_to, raw_writer):
    """Eventos: categoria (10), acao (11), nome (12) + a pagina onde ocorreram.

    Nao existe coluna idaction_event_name: o Matomo guarda o nome do evento em
    idaction_name -- a mesma coluna da pesquisa interna, distinguida pelo type
    (12 = nome de evento, 8 = pesquisa, 4 = titulo de pagina). Sem o filtro por
    type, um titulo de pagina apareceria como nome do evento.
    """
    sql = (
        "SELECT llva.idlink_va, llva.idvisit, llva.server_time, llva.idpageview,"
        " ec.name AS category, ea.name AS action, en.name AS event_name,"
        " pu.name AS page_name, pu.url_prefix AS page_url_prefix"
        " FROM {p}log_link_visit_action llva"
        " JOIN {p}log_action ec ON ec.idaction = llva.idaction_event_category AND ec.type = %s"
        " LEFT JOIN {p}log_action ea ON ea.idaction = llva.idaction_event_action AND ea.type = %s"
        " LEFT JOIN {p}log_action en ON en.idaction = llva.idaction_name AND en.type = %s"
        " LEFT JOIN {p}log_action pu ON pu.idaction = llva.idaction_url"
        " WHERE llva.idsite = %s AND llva.server_time >= %s AND llva.server_time < %s"
        " ORDER BY llva.idlink_va"
    ).format(p=prefix)

    first, last = day_from.isoformat(), day_to.isoformat()
    agg = {}
    seen = 0
    params = (
        TYPE_EVENT_CATEGORY, TYPE_EVENT_ACTION, TYPE_EVENT_NAME,
        site_id, window[0], window[1],
    )
    for row in query(conn, driver, sql, params, stream=True):
        day = bucketer.day(row["server_time"])
        if day < first or day > last:
            continue
        seen += 1
        page_url = full_url(row["page_name"], row["page_url_prefix"])
        obj_type, obj_ref = match_object(page_url)
        _accumulate(
            agg,
            (day, row["category"], row["action"], row["event_name"], obj_type, obj_ref),
            row["idvisit"],
        )
        if raw_writer is not None:
            raw_writer.write(
                {
                    "_id": int(row["idlink_va"]),
                    "site_id": site_id,
                    "idvisit": int(row["idvisit"]),
                    "server_time": utc_iso(row["server_time"]),
                    "date": day,
                    "idpageview": row["idpageview"],
                    "action_type": TYPE_EVENT_CATEGORY,
                    "event_category": row["category"],
                    "event_action": row["action"],
                    "event_name": row["event_name"],
                    "page_url": page_url,
                    "object_type": obj_type,
                    "object_ref": obj_ref,
                }
            )
    return agg, seen


# --------------------------------------------------------------------------- #
# Documentos agregados
# --------------------------------------------------------------------------- #

def agg_doc(name, site_id, key, value):
    """Constroi o documento agregado. _id = sha1 da chave natural."""
    nb_visits = len(value["visits"])
    if name == "visits_daily":
        day, _unused, obj_type, obj_ref = key
        return {
            "_id": natural_id(name, site_id, day, obj_type, obj_ref),
            "site_id": site_id,
            "date": day,
            "object_type": obj_type,
            "object_ref": obj_ref,
            "nb_visits": nb_visits,
            "nb_hits": value["hits"],
        }
    if name in ("outlinks_daily", "downloads_daily"):
        day, target, obj_type, obj_ref = key
        return {
            "_id": natural_id(name, site_id, day, target, obj_type, obj_ref),
            "site_id": site_id,
            "date": day,
            "target_url": target,
            "object_type": obj_type,
            "object_ref": obj_ref,
            "nb_visits": nb_visits,
            "nb_hits": value["hits"],
        }
    if name == "searches_daily":
        day, keyword, search_cat = key
        return {
            "_id": natural_id(name, site_id, day, keyword, search_cat),
            "site_id": site_id,
            "date": day,
            "keyword": keyword,
            "search_cat": search_cat,
            "nb_searches": value["hits"],
            "nb_visits": nb_visits,
            "nb_results": value["results"],
        }
    if name == "events_daily":
        day, category, action, event_name, obj_type, obj_ref = key
        return {
            "_id": natural_id(name, site_id, day, category, action, event_name, obj_type, obj_ref),
            "site_id": site_id,
            "date": day,
            "event_category": category,
            "event_action": action,
            "event_name": event_name,
            "object_type": obj_type,
            "object_ref": obj_ref,
            "nb_visits": nb_visits,
            "nb_hits": value["hits"],
        }
    raise ValueError("familia agregada desconhecida: %s" % name)


# --------------------------------------------------------------------------- #
# Validacao contra as tabelas de arquivo
# --------------------------------------------------------------------------- #

def verify_against_archive(conn, driver, site_id, day_from, day_to, visits_by_day, archive_tables):
    """Compara nb_visits diario (agregado dos logs) com o arquivo do Matomo.

    Sem a API web e a unica referencia independente disponivel. Compara apenas o
    total do site: o arquivo numerico nao guarda um valor por objecto.
    """
    archive = {}
    for table in sorted(archive_tables):
        month = table.rsplit("_numeric_", 1)[-1].replace("_", "-")
        if month < day_from.strftime("%Y-%m") or month > day_to.strftime("%Y-%m"):
            continue
        try:
            for row in fetch_all(
                conn,
                driver,
                "SELECT date1, value FROM {t} WHERE idsite = %s AND period = 1 "
                "AND name = 'nb_visits' AND date1 BETWEEN %s AND %s".format(t=table),
                (site_id, day_from.isoformat(), day_to.isoformat()),
            ):
                if row["date1"] is not None:
                    archive[row["date1"].isoformat()] = int(row["value"] or 0)
        except Exception as exc:  # tabela pode nao ter as colunas/indices esperados
            print("  AVISO: nao foi possivel ler %s: %s" % (table, exc), file=sys.stderr)

    # Com o export filtrado por padrao de URL, logs <= arquivo e o caso NORMAL:
    # o arquivo conta todas as visitas do site, o export so as que tocaram
    # paginas de objectos udata. Contar isso como "divergencia" era dar o alarme
    # em todos os dias e tornar o alarme inutil. So logs > arquivo e impossivel.
    matching, below, above = [], [], []
    for day in sorted(archive):
        row = {"date": day, "nb_visits_logs": visits_by_day.get(day, 0), "nb_visits_archive": archive[day]}
        if row["nb_visits_logs"] == row["nb_visits_archive"]:
            matching.append(row)
        elif row["nb_visits_logs"] < row["nb_visits_archive"]:
            below.append(row)
        else:
            above.append(row)

    return {
        "days_with_archive": len(archive),
        "days_matching": len(matching),
        "days_logs_below_archive": len(below),
        "days_logs_above_archive": len(above),
        "suspect_days": above[:200],
        "below_archive_sample": below[:50],
        "verdict": "ok" if not above else "SUSPEITO",
        "note": (
            "nb_visits_logs = COUNT(DISTINCT idvisit) das paginas exportadas; nb_visits_archive = "
            "todas as visitas do site, incluindo as que nunca tocaram uma pagina de objecto udata. "
            "logs <= arquivo e o esperado num export filtrado por padrao de URL. "
            "logs > arquivo (suspect_days) e que nao deveria acontecer."
        ),
    }


# --------------------------------------------------------------------------- #
# Export
# --------------------------------------------------------------------------- #

def resolve_range(args, coverage):
    cov = coverage["raw_log_coverage"]
    if args.all:
        if not cov["min_server_time_utc"]:
            raise SystemExit("ERRO: nao existem acoes em bruto para o site %s." % args.site_id)
        day_from = datetime.fromisoformat(cov["min_server_time_utc"]).date()
        day_to = min(date.today() - timedelta(days=1), datetime.fromisoformat(cov["max_server_time_utc"]).date())
    else:
        if not args.date_from or not args.date_to:
            raise SystemExit("ERRO: use --all, ou --date-from e --date-to (YYYY-MM-DD).")
        day_from = datetime.strptime(args.date_from, "%Y-%m-%d").date()
        day_to = datetime.strptime(args.date_to, "%Y-%m-%d").date()
    if day_from > day_to:
        raise SystemExit("ERRO: --date-from (%s) e posterior a --date-to (%s)." % (day_from, day_to))
    return day_from, day_to


def run_export(conn, driver, prefix, args, coverage):
    site_id = args.site_id
    site = next((s for s in coverage["sites"] if s["idsite"] == site_id), None)
    if site is None:
        raise SystemExit(
            "ERRO: idsite=%s nao existe. Sites disponiveis: %s"
            % (site_id, [s["idsite"] for s in coverage["sites"]])
        )

    bucketer = DayBucketer(site["timezone"], force_utc=args.utc_days)
    day_from, day_to = resolve_range(args, coverage)

    out_dir = os.path.abspath(args.out_dir or "matomo_export_%s_%s_%s" % (site_id, day_from, day_to))
    os.makedirs(out_dir, exist_ok=True)

    print("\nSite %s (%s) | fuso %s -> dias agrupados em %s" % (site_id, site["name"], site["timezone"], bucketer.mode))
    print("Intervalo: %s a %s" % (day_from, day_to))
    print("Destino:   %s" % out_dir)
    if coverage["raw_data_purge_enabled"]:
        print("AVISO: a purga de dados em bruto esta activa -- o export cobre so o que resta nos logs.")

    with open(os.path.join(out_dir, "coverage.json"), "w", encoding="utf-8") as fh:
        json.dump(coverage, fh, ensure_ascii=False, indent=2)
    # Todos os documentos exportados levam _id, incluindo estes: o importador
    # exige-o para que a escrita no Mongo seja idempotente. Aqui o idsite serve
    # de chave natural.
    # _id em primeiro lugar: o Mongo guarda-o sempre a frente, e um ReplaceOne com
    # os campos noutra ordem conta como alteracao mesmo com o conteudo igual --
    # o que estragaria a prova de idempotencia na reimportacao.
    site_writer = NdjsonWriter(os.path.join(out_dir, "site.ndjson.gz"))
    for row in coverage["sites"]:
        doc = {"_id": row["idsite"]}
        doc.update(row)
        site_writer.write(doc)
    site_writer.close()
    site_file_count = site_writer.count

    # Um ficheiro por familia agregada, escrito mes a mes. Como cada dia cai num
    # unico bloco mensal, as chaves nunca se repetem entre meses -- da para
    # libertar os agregados no fim de cada mes em vez de acumular tudo em memoria.
    agg_writers = {
        name: NdjsonWriter(os.path.join(out_dir, "agg", name + ".ndjson.gz")) for name in AGG_FAMILIES
    }
    raw_counts = defaultdict(int)
    raw_files = {}
    visits_by_day = {}
    prefilter_on = (not args.no_sql_prefilter) and not args.raw_all_actions

    try:
        chunks = month_chunks(day_from, day_to)
        for index, (month_key, chunk_from, chunk_to) in enumerate(chunks, 1):
            print("\n[%d/%d] %s  (%s a %s)" % (index, len(chunks), month_key, chunk_from, chunk_to))
            window = bucketer.utc_window(chunk_from, chunk_to)

            url_jobs = [
                ("pageviews", "visits_daily", TYPE_PAGE_URL, False),
                ("outlinks", "outlinks_daily", TYPE_OUTLINK, True),
                ("downloads", "downloads_daily", TYPE_DOWNLOAD, True),
            ]
            for family, agg_name, action_type, by_referrer in url_jobs:
                raw_writer, raw_rel = _open_raw(out_dir, family, month_key, args, raw_counts, raw_files)
                try:
                    agg, seen = extract_url_family(
                        conn, driver, prefix, site_id, action_type, window, bucketer,
                        chunk_from, chunk_to,
                        attribute_by_referrer=by_referrer,
                        # O prefiltro SQL casa a.name (o URL da propria acao); nos
                        # outlinks/downloads o objecto vem do referrer, logo filtrar
                        # por a.name deitaria fora exactamente o que queremos.
                        use_prefilter=prefilter_on and not by_referrer,
                        keep_unmatched=args.raw_all_actions or by_referrer,
                        raw_writer=raw_writer,
                    )
                finally:
                    raw_written = _close_raw(raw_writer, raw_rel, family, raw_counts, raw_files)

                if family == "pageviews":
                    # Total de visitas por dia, para o --verify-archive. As visitas
                    # tem de ser deduplicadas entre objectos: a mesma visita pode
                    # ter passado por varios datasets.
                    day_visits = defaultdict(set)
                    for key, value in agg.items():
                        day_visits[key[0]].update(value["visits"])
                    for day, visits in day_visits.items():
                        visits_by_day[day] = len(visits)

                written = _flush_agg(agg_writers[agg_name], agg_name, site_id, agg)
                print("    %-10s %7d lidas  %7s em bruto  %6d agregados"
                      % (family, seen, "-" if raw_written is None else raw_written, written))
                del agg

            raw_writer, raw_rel = _open_raw(out_dir, "searches", month_key, args, raw_counts, raw_files)
            try:
                agg, seen = extract_searches(
                    conn, driver, prefix, site_id, window, bucketer, chunk_from, chunk_to, raw_writer
                )
            finally:
                raw_written = _close_raw(raw_writer, raw_rel, "searches", raw_counts, raw_files)
            written = _flush_agg(agg_writers["searches_daily"], "searches_daily", site_id, agg)
            print("    %-10s %7d lidas  %7s em bruto  %6d agregados"
                  % ("searches", seen, "-" if raw_written is None else raw_written, written))
            del agg

            raw_writer, raw_rel = _open_raw(out_dir, "events", month_key, args, raw_counts, raw_files)
            try:
                agg, seen = extract_events(
                    conn, driver, prefix, site_id, window, bucketer, chunk_from, chunk_to, raw_writer
                )
            finally:
                raw_written = _close_raw(raw_writer, raw_rel, "events", raw_counts, raw_files)
            written = _flush_agg(agg_writers["events_daily"], "events_daily", site_id, agg)
            print("    %-10s %7d lidas  %7s em bruto  %6d agregados"
                  % ("events", seen, "-" if raw_written is None else raw_written, written))
            del agg
    finally:
        for writer in agg_writers.values():
            writer.close()

    agg_counts = {name: writer.count for name, writer in agg_writers.items()}
    print("\nAgregados:")
    for name in AGG_FAMILIES:
        print("  %-16s %8d documentos" % (name, agg_counts[name]))

    verification = None
    if args.verify_archive:
        print("\nA comparar com matomo_archive_numeric_* ...")
        verification = verify_against_archive(
            conn, driver, site_id, day_from, day_to, visits_by_day,
            coverage["archive_tables"].get("numeric", []),
        )
        print(
            "  %d dias com arquivo construido: %d iguais, %d abaixo (esperado), %d ACIMA"
            % (verification["days_with_archive"], verification["days_matching"],
               verification["days_logs_below_archive"], verification["days_logs_above_archive"])
        )
        print("  veredicto: %s" % verification["verdict"])
        if verification["days_logs_above_archive"]:
            print("  ATENCAO: ha dias com mais visitas nos logs do que no arquivo -- ver suspect_days no manifesto.")

    manifest = {
        "script": os.path.basename(__file__),
        "script_version": SCRIPT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "kind": "matomo-mariadb",
            "mariadb_version": coverage["mariadb_version"],
            "matomo_version": coverage["matomo_version"],
            "table_prefix": prefix,
            "driver": driver,
        },
        "site": site,
        "site_id": site_id,
        "date_from": day_from.isoformat(),
        "date_to": day_to.isoformat(),
        "day_bucketing": bucketer.mode,
        "timezone": site["timezone"],
        "options": {
            "raw_included": not args.no_raw,
            "raw_all_actions": args.raw_all_actions,
            "sql_prefilter": prefilter_on,
            "utc_days": args.utc_days,
        },
        "counts": {
            "agg": agg_counts,
            "raw": dict(raw_counts),
            "agg_total": sum(agg_counts.values()),
            "raw_total": sum(raw_counts.values()),
        },
        "collections": {
            "agg/visits_daily.ndjson.gz": "visits_daily",
            "agg/outlinks_daily.ndjson.gz": "outlinks_daily",
            "agg/downloads_daily.ndjson.gz": "downloads_daily",
            "agg/searches_daily.ndjson.gz": "searches_daily",
            "agg/events_daily.ndjson.gz": "events_daily",
            "site.ndjson.gz": "site",
        },
        "files": {},
        "verification": verification,
    }

    known_counts = dict(raw_files)
    known_counts.update({"agg/%s.ndjson.gz" % n: agg_counts[n] for n in AGG_FAMILIES})
    known_counts["site.ndjson.gz"] = site_file_count

    for rel in sorted(set(list(known_counts) + ["coverage.json"])):
        abs_path = os.path.join(out_dir, rel)
        if not os.path.exists(abs_path):
            continue
        manifest["files"][rel] = {
            "sha256": sha256_file(abs_path),
            "bytes": os.path.getsize(abs_path),
            "documents": known_counts.get(rel),
        }

    manifest_path = os.path.join(out_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "checksums.sha256"), "w", encoding="utf-8") as fh:
        for rel, meta in sorted(manifest["files"].items()):
            fh.write("%s  %s\n" % (meta["sha256"], rel))

    print("\nManifesto: %s" % manifest_path)
    print("Total: %d documentos agregados, %d acoes em bruto"
          % (manifest["counts"]["agg_total"], manifest["counts"]["raw_total"]))

    if args.tar:
        tar_path = out_dir + ".tar.gz"
        print("A empacotar %s ..." % tar_path)
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(out_dir, arcname=os.path.basename(out_dir))
        print("Copiar para a VM do MongoDB:  scp %s <vm-mongo>:/tmp/" % tar_path)

    return out_dir


def _open_raw(out_dir, family, month_key, args, raw_counts, raw_files):
    """Abre o ficheiro de acoes em bruto do mes, ou None se nao for para escrever."""
    if args.no_raw:
        return None, None
    rel = os.path.join("raw", family, month_key + ".ndjson.gz")
    path = os.path.join(out_dir, rel)
    if os.path.exists(path) and not args.overwrite:
        # Retomar um export interrompido: manter o ficheiro, contando as linhas
        # que ja tem. Sem isto, o total do manifesto reportaria zero em bruto num
        # export retomado, quando os dados estao todos la.
        existing = count_ndjson_lines(path)
        raw_files[rel] = existing
        raw_counts[family] += existing
        print("    %-10s %s ja existe com %d linhas -- mantido (--overwrite para refazer)"
              % (family, rel, existing))
        return None, None
    return NdjsonWriter(path), rel


def _close_raw(raw_writer, rel, family, raw_counts, raw_files):
    """Fecha o ficheiro em bruto e devolve quantas linhas escreveu."""
    if raw_writer is None:
        return None
    raw_writer.close()
    raw_counts[family] += raw_writer.count
    raw_files[rel] = raw_writer.count
    return raw_writer.count


def _flush_agg(writer, name, site_id, agg):
    before = writer.count
    for key in sorted(agg, key=lambda k: tuple("" if p is None else str(p) for p in k)):
        writer.write(agg_doc(name, site_id, key, agg[key]))
    return writer.count - before


# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(
        description="Exporta metricas do Matomo directamente da MariaDB (read-only).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config-ini", help="config.ini.php do Matomo (le a seccao [database])")
    parser.add_argument("--db-host", help="host da MariaDB (env MATOMO_DB_HOST)")
    parser.add_argument("--db-port", type=int, help="porta (env MATOMO_DB_PORT)")
    parser.add_argument("--db-user", help="utilizador (env MATOMO_DB_USER)")
    parser.add_argument("--db-password", help="password (env MATOMO_DB_PASSWORD)")
    parser.add_argument("--db-database", help="base de dados (env MATOMO_DB_NAME, def. matomo)")
    parser.add_argument("--db-prefix", help="prefixo das tabelas (env MATOMO_DB_PREFIX, def. matomo_)")

    parser.add_argument("--site-id", type=int, help="idsite do Matomo (env MATOMO_SITE_ID)")
    parser.add_argument("--info", action="store_true", help="so diagnostico, nao exporta")
    parser.add_argument("--date-from", help="primeiro dia a exportar (YYYY-MM-DD)")
    parser.add_argument("--date-to", help="ultimo dia a exportar (YYYY-MM-DD)")
    parser.add_argument("--all", action="store_true", help="todo o historico existente nos logs")
    parser.add_argument("--out-dir", help="directorio de destino")
    parser.add_argument("--tar", action="store_true", help="empacota o resultado num .tar.gz")
    parser.add_argument("--no-raw", action="store_true", help="exporta so os agregados")
    parser.add_argument("--raw-all-actions", action="store_true",
                        help="exporta todas as acoes, nao so as que casam com os padroes de URL")
    parser.add_argument("--no-sql-prefilter", action="store_true",
                        help="nao filtra por LIKE no SQL (mais trafego, util para depurar)")
    parser.add_argument("--verify-archive", action="store_true",
                        help="compara nb_visits diario com matomo_archive_numeric_*")
    parser.add_argument("--overwrite", action="store_true", help="refaz meses ja exportados")
    parser.add_argument("--utc-days", action="store_true",
                        help="agrupa os dias em UTC em vez do fuso do site")
    args = parser.parse_args()

    if args.site_id is None and os.environ.get("MATOMO_SITE_ID"):
        args.site_id = int(os.environ["MATOMO_SITE_ID"])
    if args.site_id is None and not args.info:
        raise SystemExit("ERRO: indique --site-id (ou MATOMO_SITE_ID). Use --info para listar os sites.")

    cfg = resolve_db_config(args)
    print("A ligar a MariaDB %s:%s/%s (prefixo %s)..." % (cfg["host"], cfg["port"], cfg["database"], cfg["prefix"]))
    conn, driver = connect(cfg)
    print("  driver: %s" % driver)

    try:
        # Leitura consistente: todas as consultas veem o mesmo instante. Nada e escrito.
        cur = conn.cursor()
        try:
            cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        finally:
            cur.close()

        coverage = collect_coverage(conn, driver, cfg["prefix"], args.site_id)
        print()
        print_coverage(coverage)

        if args.info:
            out = args.out_dir or "."
            os.makedirs(out, exist_ok=True)
            path = os.path.join(out, "coverage.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(coverage, fh, ensure_ascii=False, indent=2)
            print("\nEscrito %s" % os.path.abspath(path))
            return 0

        run_export(conn, driver, cfg["prefix"], args, coverage)
        return 0
    finally:
        try:
            conn.rollback()  # nada foi escrito; so fecha a transacao de leitura
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
