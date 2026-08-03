#!/usr/bin/env python3
"""
Carrega no MongoDB um export produzido por export_matomo_mariadb.py.

Corre NA VM DO MONGODB, sobre o directorio (ou .tar.gz) copiado da VM do Matomo.

Faz duas coisas, por esta ordem:

  1) Staging fiel -- escreve os documentos tal como vieram, numa base de dados
     propria (por omissao "matomo"). Cada documento traz um _id determinista
     calculado no exportador, logo reimportar o mesmo export nao duplica nada.

  2) Resolucao slug -> ObjectId (opcional, --resolve-udata) -- acrescenta
     object_id / dataset_id / organization_id aos documentos de staging, usando
     a mesma logica de import_matomo_metrics.py:90. Le o udata, nunca o escreve.

NAO escreve nas colecoes do udata (dataset.metrics.*, metrics, site): essas
pertencem ao DAG metrics_etl, que as reescreve a cada 15 minutos. Escrever daqui
criaria um terceiro autor no mesmo campo.

Uso:
    # 1) Ver o que o export tem, sem escrever nada
    python3 import_matomo_export_to_mongo.py /tmp/matomo_export_3_2018-07-19_2026-07-30.tar.gz --dry-run

    # 2) Importar so os agregados
    python3 import_matomo_export_to_mongo.py /tmp/matomo_export_3_... --only agg

    # 3) Importar tudo e resolver os slugs contra o udata
    python3 import_matomo_export_to_mongo.py /tmp/matomo_export_3_... --resolve-udata

Requisitos: Python 3.9+ e pymongo (pip install pymongo).
"""

import argparse
import gzip
import hashlib
import json
import os
import shutil
import sys
import tarfile
import tempfile
from collections import defaultdict
from datetime import datetime, timezone

SCRIPT_VERSION = "1.0.0"

# Ficheiro do export -> colecao de destino. As familias em bruto vao todas para
# actions_raw, distinguidas por action_type (o _id e o idlink_va, unico no Matomo).
RAW_COLLECTION = "actions_raw"
AGG_COLLECTIONS = {
    "visits_daily": "visits_daily",
    "outlinks_daily": "outlinks_daily",
    "downloads_daily": "downloads_daily",
    "searches_daily": "searches_daily",
    "events_daily": "events_daily",
}

# Indices por colecao. O _id ja garante a unicidade, portanto estes sao so para
# leitura eficiente por dia / por objecto.
INDEXES = {
    "visits_daily": [[("site_id", 1), ("date", 1)], [("object_type", 1), ("object_ref", 1)], [("object_id", 1)]],
    "outlinks_daily": [[("site_id", 1), ("date", 1)], [("object_type", 1), ("object_ref", 1)]],
    "downloads_daily": [[("site_id", 1), ("date", 1)], [("object_type", 1), ("object_ref", 1)]],
    "searches_daily": [[("site_id", 1), ("date", 1)]],
    "events_daily": [[("site_id", 1), ("date", 1)], [("event_category", 1)]],
    RAW_COLLECTION: [[("site_id", 1), ("date", 1)], [("action_type", 1), ("date", 1)], [("idvisit", 1)]],
    "site": [],
    "imports": [[("finished_at", -1)]],
    "unresolved": [[("object_type", 1), ("object_ref", 1)]],
}


# --------------------------------------------------------------------------- #
# Leitura do export
# --------------------------------------------------------------------------- #

def open_export(path):
    """Aceita um directorio ou um .tar.gz. Devolve (dir, tempdir_a_limpar)."""
    path = os.path.abspath(path)
    if os.path.isdir(path):
        return path, None
    if not tarfile.is_tarfile(path):
        raise SystemExit("ERRO: %s nao e um directorio nem um .tar.gz valido." % path)

    tmp = tempfile.mkdtemp(prefix="matomo_export_")
    print("A extrair %s para %s ..." % (path, tmp))
    with tarfile.open(path, "r:*") as tar:
        for member in tar.getmembers():
            # Nunca extrair fora do directorio temporario.
            target = os.path.realpath(os.path.join(tmp, member.name))
            if not target.startswith(os.path.realpath(tmp) + os.sep):
                raise SystemExit("ERRO: entrada suspeita no arquivo: %s" % member.name)
        try:
            tar.extractall(tmp, filter="data")  # Python 3.12+, e backports RHEL
        except TypeError:
            tar.extractall(tmp)

    entries = [os.path.join(tmp, e) for e in os.listdir(tmp)]
    roots = [e for e in entries if os.path.isdir(e) and os.path.exists(os.path.join(e, "manifest.json"))]
    if len(roots) == 1:
        return roots[0], tmp
    if os.path.exists(os.path.join(tmp, "manifest.json")):
        return tmp, tmp
    raise SystemExit("ERRO: nao encontrei manifest.json no arquivo extraido.")


def load_manifest(export_dir):
    path = os.path.join(export_dir, "manifest.json")
    if not os.path.exists(path):
        raise SystemExit(
            "ERRO: %s nao tem manifest.json -- nao parece um export de export_matomo_mariadb.py." % export_dir
        )
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_checksums(export_dir, manifest, skip=False):
    """Confirma que os ficheiros chegaram intactos. Aborta se nao chegaram."""
    files = manifest.get("files", {})
    if skip:
        print("Verificacao de checksums SALTADA (--skip-checksums).")
        return
    if not files:
        print("AVISO: o manifesto nao lista ficheiros -- nao ha nada para verificar.")
        return

    print("A verificar %d ficheiros..." % len(files))
    missing, corrupt = [], []
    for rel, meta in sorted(files.items()):
        path = os.path.join(export_dir, rel)
        if not os.path.exists(path):
            missing.append(rel)
            continue
        if sha256_file(path) != meta["sha256"]:
            corrupt.append(rel)
    if missing or corrupt:
        for rel in missing:
            print("  FALTA:     %s" % rel, file=sys.stderr)
        for rel in corrupt:
            print("  CORROMPIDO: %s" % rel, file=sys.stderr)
        raise SystemExit(
            "ERRO: o export nao esta intacto (%d em falta, %d corrompidos). "
            "Copie-o outra vez ou use --skip-checksums se souber o que esta a fazer."
            % (len(missing), len(corrupt))
        )
    print("  todos os checksums coincidem")


def iter_ndjson(path):
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit("ERRO: %s linha %d nao e JSON valido: %s" % (path, line_no, exc))


def discover_files(export_dir, only):
    """Lista (caminho, colecao, etiqueta) dos ficheiros a importar."""
    jobs = []

    if only in (None, "agg"):
        for name, collection in AGG_COLLECTIONS.items():
            path = os.path.join(export_dir, "agg", name + ".ndjson.gz")
            if os.path.exists(path):
                jobs.append((path, collection, "agg/" + name))
        site_path = os.path.join(export_dir, "site.ndjson.gz")
        if os.path.exists(site_path):
            jobs.append((site_path, "site", "site"))

    if only in (None, "raw"):
        raw_root = os.path.join(export_dir, "raw")
        if os.path.isdir(raw_root):
            for family in sorted(os.listdir(raw_root)):
                family_dir = os.path.join(raw_root, family)
                if not os.path.isdir(family_dir):
                    continue
                for month_file in sorted(os.listdir(family_dir)):
                    if month_file.endswith(".ndjson.gz"):
                        jobs.append((
                            os.path.join(family_dir, month_file),
                            RAW_COLLECTION,
                            "raw/%s/%s" % (family, month_file[: -len(".ndjson.gz")]),
                        ))
    return jobs


# --------------------------------------------------------------------------- #
# Escrita no MongoDB
# --------------------------------------------------------------------------- #

def mongo_client(args):
    try:
        from pymongo import MongoClient
    except ImportError:
        raise SystemExit("ERRO: pymongo nao esta instalado.  pip install pymongo")

    if args.mongo_uri:
        return MongoClient(args.mongo_uri)
    return MongoClient(args.mongo_host, args.mongo_port)


def ensure_indexes(db, collections):
    for name in collections:
        for keys in INDEXES.get(name, []):
            db[name].create_index(keys, background=True)


def load_file(db, collection, path, batch_size, dry_run):
    """Carrega um NDJSON com ReplaceOne(upsert) em lotes.

    O _id vem do exportador, por isso reimportar o mesmo ficheiro substitui os
    mesmos documentos em vez de os duplicar.
    """
    from pymongo import ReplaceOne
    from pymongo.errors import BulkWriteError

    stats = {"read": 0, "upserted": 0, "modified": 0, "matched": 0}
    batch = []

    def flush():
        if not batch or dry_run:
            batch.clear()
            return
        try:
            result = db[collection].bulk_write(batch, ordered=False)
            stats["upserted"] += result.upserted_count
            stats["modified"] += result.modified_count
            stats["matched"] += result.matched_count
        except BulkWriteError as exc:
            errors = exc.details.get("writeErrors", [])
            print("  ERRO: %d escritas falharam; primeira: %s"
                  % (len(errors), errors[0].get("errmsg") if errors else "?"), file=sys.stderr)
            raise
        batch.clear()

    for doc in iter_ndjson(path):
        stats["read"] += 1
        if "_id" not in doc:
            raise SystemExit("ERRO: documento sem _id em %s -- export incompativel." % path)
        batch.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        if len(batch) >= batch_size:
            flush()
    flush()
    return stats


# --------------------------------------------------------------------------- #
# Resolucao slug -> ObjectId (contra o udata)
# --------------------------------------------------------------------------- #

def build_slug_to_oid_lookup(client, udata_db_name):
    """Constroi o mapa (tipo, slug|oid) -> {id, dataset_id?, organization_id?}.

    Mesma logica de import_matomo_metrics.py:90-145, para que os identificadores
    resolvidos aqui sejam exactamente os mesmos que o pipeline antigo produzia.
    Leitura apenas.
    """
    db = client[udata_db_name]
    lookup = {}

    for doc in db["dataset"].find({"deleted": None}, {"slug": 1, "organization": 1}):
        oid = str(doc["_id"])
        entry = {
            "id": oid,
            "organization_id": str(doc["organization"]) if doc.get("organization") else None,
        }
        if doc.get("slug"):
            lookup[("datasets", doc["slug"])] = entry
        lookup[("datasets", oid)] = entry

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

    for doc in db["organization"].find({"deleted": None}, {"slug": 1}):
        oid = str(doc["_id"])
        entry = {"id": oid}
        if doc.get("slug"):
            lookup[("organizations", doc["slug"])] = entry
        lookup[("organizations", oid)] = entry

    for doc in db["reuse"].find({"deleted": None}, {"slug": 1, "organization": 1}):
        oid = str(doc["_id"])
        entry = {
            "id": oid,
            "organization_id": str(doc["organization"]) if doc.get("organization") else None,
        }
        if doc.get("slug"):
            lookup[("reuses", doc["slug"])] = entry
        lookup[("reuses", oid)] = entry

    if "dataservice" in db.list_collection_names():
        for doc in db["dataservice"].find({"deleted": None}, {"slug": 1, "organization": 1}):
            oid = str(doc["_id"])
            entry = {
                "id": oid,
                "organization_id": str(doc["organization"]) if doc.get("organization") else None,
            }
            if doc.get("slug"):
                lookup[("dataservices", doc["slug"])] = entry
            lookup[("dataservices", oid)] = entry

    return lookup


def resolve_collection(db, collection, lookup, batch_size, dry_run):
    """Acrescenta object_id / dataset_id / organization_id aos documentos.

    Os que nao resolvem sao contados e registados em "unresolved" -- ao contrario
    do `continue` silencioso em import_matomo_metrics.py:209, que fazia
    desaparecer sem rasto tudo o que nao estava no mapa.
    """
    from pymongo import UpdateOne

    stats = {"scanned": 0, "resolved": 0, "unresolved": 0}
    unresolved = defaultdict(int)
    batch = []

    def flush():
        if batch and not dry_run:
            db[collection].bulk_write(batch, ordered=False)
        batch.clear()

    cursor = db[collection].find(
        {"object_type": {"$ne": None}},
        {"object_type": 1, "object_ref": 1},
        no_cursor_timeout=False,
    )
    for doc in cursor:
        stats["scanned"] += 1
        key = (doc.get("object_type"), doc.get("object_ref"))
        info = lookup.get(key)
        if not info:
            stats["unresolved"] += 1
            unresolved[key] += 1
            continue
        update = {"object_id": info["id"]}
        if info.get("dataset_id"):
            update["dataset_id"] = info["dataset_id"]
        if info.get("organization_id"):
            update["organization_id"] = info["organization_id"]
        batch.append(UpdateOne({"_id": doc["_id"]}, {"$set": update}))
        stats["resolved"] += 1
        if len(batch) >= batch_size:
            flush()
    flush()

    if unresolved and not dry_run:
        from pymongo import ReplaceOne

        docs = [
            ReplaceOne(
                {"_id": "%s:%s:%s" % (collection, obj_type, obj_ref)},
                {
                    "_id": "%s:%s:%s" % (collection, obj_type, obj_ref),
                    "collection": collection,
                    "object_type": obj_type,
                    "object_ref": obj_ref,
                    "documents": count,
                    "seen_at": datetime.now(timezone.utc),
                },
                upsert=True,
            )
            for (obj_type, obj_ref), count in unresolved.items()
        ]
        for start in range(0, len(docs), batch_size):
            db["unresolved"].bulk_write(docs[start:start + batch_size], ordered=False)

    stats["unresolved_distinct"] = len(unresolved)
    stats["unresolved_examples"] = [
        {"object_type": k[0], "object_ref": k[1], "documents": v}
        for k, v in sorted(unresolved.items(), key=lambda kv: -kv[1])[:10]
    ]
    return stats


# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(
        description="Carrega no MongoDB um export de export_matomo_mariadb.py.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("export", help="directorio do export ou .tar.gz")
    parser.add_argument("--mongo-uri", help="URI completo (tem precedencia sobre host/porta)")
    parser.add_argument("--mongo-host", default=os.environ.get("MONGODB_HOST", "127.0.0.1"),
                        help="host do MongoDB (env MONGODB_HOST)")
    parser.add_argument("--mongo-port", type=int, default=int(os.environ.get("MONGODB_PORT", "27017")),
                        help="porta do MongoDB (env MONGODB_PORT)")
    parser.add_argument("--mongo-db", default="matomo", help="base de dados de staging (def. matomo)")
    parser.add_argument("--only", choices=["agg", "raw"], help="importar so os agregados ou so as acoes em bruto")
    parser.add_argument("--batch-size", type=int, default=1000, help="documentos por lote (def. 1000)")
    parser.add_argument("--dry-run", action="store_true", help="le e conta, nao escreve nada")
    parser.add_argument("--skip-checksums", action="store_true", help="nao verificar os sha256 (nao recomendado)")
    parser.add_argument("--resolve-udata", action="store_true",
                        help="resolve slug -> ObjectId contra o udata e preenche object_id")
    parser.add_argument("--udata-db", default="udata", help="base de dados do udata (def. udata)")
    parser.add_argument("--udata-uri", help="URI do Mongo do udata, se for outro servidor")
    args = parser.parse_args()

    export_dir, tmp_dir = open_export(args.export)
    try:
        manifest = load_manifest(export_dir)
        print("Export: site %s (%s), %s a %s"
              % (manifest["site_id"], manifest["site"]["name"], manifest["date_from"], manifest["date_to"]))
        print("  gerado em %s por %s v%s"
              % (manifest["generated_at"], manifest["script"], manifest["script_version"]))
        print("  dias agrupados em: %s (%s)" % (manifest["day_bucketing"], manifest["timezone"]))
        print("  %s documentos agregados, %s acoes em bruto"
              % (manifest["counts"]["agg_total"], manifest["counts"]["raw_total"]))
        if manifest.get("verification"):
            ver = manifest["verification"]
            print("  validacao contra o arquivo do Matomo: %s (%d dias, %d iguais, %d abaixo, %d acima)"
                  % (ver.get("verdict", "?"), ver.get("days_with_archive", 0), ver.get("days_matching", 0),
                     ver.get("days_logs_below_archive", 0), ver.get("days_logs_above_archive", 0)))

        verify_checksums(export_dir, manifest, skip=args.skip_checksums)

        jobs = discover_files(export_dir, args.only)
        if not jobs:
            raise SystemExit("ERRO: nada para importar (--only %s)." % args.only)

        client = mongo_client(args)
        db = client[args.mongo_db]
        print("\nDestino: %s / base de dados '%s'%s"
              % (args.mongo_uri or "%s:%s" % (args.mongo_host, args.mongo_port),
                 args.mongo_db, "  [DRY-RUN, nada sera escrito]" if args.dry_run else ""))

        collections = sorted({collection for _, collection, _ in jobs})
        if not args.dry_run:
            ensure_indexes(db, collections + ["imports", "unresolved"])

        started = datetime.now(timezone.utc)
        totals = defaultdict(lambda: {"read": 0, "upserted": 0, "modified": 0, "matched": 0})
        print()
        for path, collection, label in jobs:
            stats = load_file(db, collection, path, args.batch_size, args.dry_run)
            for key, value in stats.items():
                totals[collection][key] += value
            print("  %-28s -> %-14s %7d lidos, %7d novos, %7d actualizados"
                  % (label, collection, stats["read"], stats["upserted"], stats["modified"]))

        print("\nPor colecao:")
        for collection in sorted(totals):
            stats = totals[collection]
            count = None if args.dry_run else db[collection].count_documents({})
            print("  %-16s %8d lidos, %8d novos, %8d actualizados%s"
                  % (collection, stats["read"], stats["upserted"], stats["modified"],
                     "  (total na colecao: %d)" % count if count is not None else ""))

        resolution = None
        if args.resolve_udata:
            print("\nA construir o mapa slug -> ObjectId a partir do udata...")
            udata_client = client
            if args.udata_uri:
                from pymongo import MongoClient

                udata_client = MongoClient(args.udata_uri)
            lookup = build_slug_to_oid_lookup(udata_client, args.udata_db)
            print("  %d entradas no mapa" % len(lookup))

            resolution = {}
            for collection in sorted(totals):
                if collection in ("site",):
                    continue
                stats = resolve_collection(db, collection, lookup, args.batch_size, args.dry_run)
                if stats["scanned"] == 0:
                    continue
                resolution[collection] = stats
                print("  %-16s %8d com objecto, %8d resolvidos, %8d nao resolvidos (%d distintos)"
                      % (collection, stats["scanned"], stats["resolved"],
                         stats["unresolved"], stats["unresolved_distinct"]))
                for example in stats["unresolved_examples"][:5]:
                    print("      nao resolvido: %s/%s (%d docs)"
                          % (example["object_type"], example["object_ref"], example["documents"]))
            if udata_client is not client:
                udata_client.close()

        if not args.dry_run:
            db["imports"].insert_one({
                "script": os.path.basename(__file__),
                "script_version": SCRIPT_VERSION,
                "started_at": started,
                "finished_at": datetime.now(timezone.utc),
                "export": {
                    "path": os.path.abspath(args.export),
                    "site_id": manifest["site_id"],
                    "date_from": manifest["date_from"],
                    "date_to": manifest["date_to"],
                    "generated_at": manifest["generated_at"],
                    "manifest_counts": manifest["counts"],
                },
                "only": args.only,
                "collections": {k: dict(v) for k, v in totals.items()},
                "resolution": resolution,
            })

        client.close()
        print("\nConcluido%s." % (" (dry-run)" if args.dry_run else ""))
        return 0
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
