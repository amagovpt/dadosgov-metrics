#!/usr/bin/env python3
"""
Carrega no MongoDB um export produzido por export_matomo_mariadb.py, ou
directamente um dump .sql.gz da MariaDB do Matomo.

Corre NA VM DO MONGODB, sobre o directorio (ou .tar.gz) copiado da VM do Matomo,
ou sobre um backup .sql/.sql.gz do mysqldump.

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

Dois formatos de entrada:

  a) Export de export_matomo_mariadb.py -- directorio ou .tar.gz. Caminho normal.

  b) Dump do mysqldump -- .sql, .sql.gz, .sql.bz2 ou .sql.xz. Para quando o que
     existe e um backup da MariaDB e nao acesso a base de dados do Matomo. O dump
     e carregado para uma base sqlite temporaria e as MESMAS funcoes de extraccao
     de export_matomo_mariadb.py correm sobre ela, produzindo um export
     intermedio que depois sobe para o Mongo pelo caminho (a). Nao ha uma segunda
     implementacao das metricas: os documentos e os _id deterministas sao iguais
     aos de um export feito na VM do Matomo, logo os dois caminhos substituem-se
     em vez de duplicarem. Precisa do export_matomo_mariadb.py ao lado deste
     script (nao precisa de driver MySQL nem de servidor MariaDB).

Uso:
    # 1) Ver o que o export tem, sem escrever nada
    python3 import_matomo_export_to_mongo.py /tmp/matomo_export_3_2018-07-19_2026-07-30.tar.gz --dry-run

    # 2) Importar so os agregados
    python3 import_matomo_export_to_mongo.py /tmp/matomo_export_3_... --only agg

    # 3) Importar tudo e resolver os slugs contra o udata
    python3 import_matomo_export_to_mongo.py /tmp/matomo_export_3_... --resolve-udata

    # 4) Importar a partir de um backup .sql.gz da MariaDB do Matomo
    python3 import_matomo_export_to_mongo.py /backups/matomo-2026-07-30.sql.gz \\
        --site-id 3 --dry-run
    python3 import_matomo_export_to_mongo.py /backups/matomo-2026-07-30.sql.gz \\
        --site-id 3 --staging-dir /var/tmp/matomo-staging --resolve-udata

Requisitos: Python 3.9+ e pymongo (pip install pymongo).
"""

import argparse
import gzip
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
import tarfile
import tempfile
from collections import defaultdict
from datetime import date, datetime, timezone

SCRIPT_VERSION = "1.1.0"

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
# Entrada alternativa: dump do mysqldump (.sql / .sql.gz)
# --------------------------------------------------------------------------- #
#
# O dump e carregado para uma base sqlite temporaria com so as tabelas e colunas
# de que as metricas precisam, e depois as funcoes de extraccao do
# export_matomo_mariadb.py correm sobre essa base atraves de um adaptador que
# imita a interface do driver MySQL. E a razao de nao haver aqui uma segunda
# implementacao das metricas: a agregacao, o agrupamento por dia no fuso do site
# e o calculo dos _id continuam a viver num sitio so.

def load_exporter_module():
    """Importa o export_matomo_mariadb.py, que tem de estar ao lado deste script.

    So o caminho .sql.gz depende dele -- importar um export normal continua a
    funcionar com este ficheiro sozinho, e por isso o import e feito aqui e nao
    no topo do modulo.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    if here not in sys.path:
        sys.path.insert(0, here)
    try:
        import export_matomo_mariadb as exporter
    except ImportError as exc:
        raise SystemExit(
            "ERRO: para importar um dump .sql.gz e preciso o export_matomo_mariadb.py\n"
            "      no mesmo directorio que este script (%s).\n"
            "      Detalhe: %s" % (here, exc)
        )
    return exporter


def open_dump_text(path):
    """Abre um dump em modo texto, descomprimindo pelos bytes magicos."""
    with open(path, "rb") as probe:
        magic = probe.read(6)
    if magic.startswith(b"\x1f\x8b"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    if magic.startswith(b"BZh"):
        import bz2

        return bz2.open(path, "rt", encoding="utf-8", errors="replace")
    if magic.startswith(b"\xfd7zXZ\x00"):
        import lzma

        return lzma.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def looks_like_sql_dump(path):
    """Distingue um dump SQL de um export (directorio ou .tar.gz)."""
    if os.path.isdir(path):
        return False
    if path.lower().endswith((".sql", ".sql.gz", ".sql.bz2", ".sql.xz")):
        return True
    try:
        if tarfile.is_tarfile(path):
            return False
    except (OSError, tarfile.TarError):
        pass
    try:
        with open_dump_text(path) as fh:
            head = fh.read(65536).upper()
    except OSError:
        return False
    return any(m in head for m in ("MYSQL DUMP", "MARIADB DUMP", "CREATE TABLE", "INSERT INTO"))


def read_dump_header(path, max_lines=60):
    """Le os comentarios de cabecalho do mysqldump (versao do servidor, base)."""
    info = {}
    try:
        with open_dump_text(path) as fh:
            for index, line in enumerate(fh):
                if index >= max_lines:
                    break
                if not line.startswith("--"):
                    continue
                if "Server version" in line:
                    info["server_version"] = line.split("Server version", 1)[1].strip()
                elif "Database:" in line:
                    found = re.search(r"Database:\s*(\S+)", line)
                    if found:
                        info["database"] = found.group(1)
                elif "MySQL dump" in line or "MariaDB dump" in line:
                    info["dump_tool"] = line.lstrip("- ").strip()
    except OSError as exc:
        raise SystemExit("ERRO: nao foi possivel ler %s: %s" % (path, exc))
    return info


# --------------------------------------------------------------------------- #
# Leitura do dump: statements e valores
# --------------------------------------------------------------------------- #

_INSERT_TABLE_RE = re.compile(r"(?:INSERT|REPLACE)\s+INTO\s+`?([^`\s(]+)`?", re.I)
_INSERT_HEADER_RE = re.compile(
    r"^\s*(?:INSERT|REPLACE)\s+(?:LOW_PRIORITY\s+|DELAYED\s+|HIGH_PRIORITY\s+|IGNORE\s+)*"
    r"INTO\s+`?([^`\s(]+)`?\s*(?:\(([^)]*)\)\s*)?VALUES\s*",
    re.I,
)
_CREATE_TABLE_RE = re.compile(r"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([^`\s(]+)`?", re.I)
_COLUMN_DEF_RE = re.compile(r"^\s*`([^`]+)`\s+[A-Za-z]")

# Sequencias de escape do mysqldump. Qualquer outra \c vale c.
_MYSQL_ESCAPES = {
    "0": "\0", "b": "\b", "n": "\n", "r": "\r", "t": "\t", "Z": "\x1a",
    "'": "'", '"': '"', "\\": "\\",
}
_NUMBER_TAIL = set("0123456789.eE+-")


def _scan_line(line, in_string):
    """Percorre uma linha e devolve (dentro_de_literal, o_statement_terminou).

    Percorrer e preciso: o ';' que termina o statement pode aparecer tambem
    dentro de um valor de texto, e ai nao termina nada.
    """
    i, size, last = 0, len(line), ""
    while i < size:
        char = line[i]
        if in_string:
            if char == "\\":
                i += 2
            elif char == "'":
                in_string = False
                last = "'"
                i += 1
            else:
                i += 1
            continue
        if char == "'":
            in_string = True
            i += 1
        elif char == "`":
            close = line.find("`", i + 1)
            i = size if close < 0 else close + 1
            last = "`"
        elif char == "#":
            break
        elif char == "-" and line.startswith("--", i) and (i + 2 >= size or line[i + 2] in " \t\r\n"):
            break
        elif char == "/" and line.startswith("/*", i):
            close = line.find("*/", i + 2)
            if close < 0:
                break
            i = close + 2
        else:
            if not char.isspace():
                last = char
            i += 1
    return in_string, (not in_string and last == ";")


def iter_dump_statements(fh, keep_table=None):
    """Devolve os CREATE TABLE e INSERT do dump, um a um.

    Nao carrega o ficheiro em memoria: o mysqldump escreve cada INSERT numa unica
    linha e escapa as mudancas de linha dentro dos literais, logo acumular linha a
    linha ate ao ';' mantem o pico de memoria no tamanho de um statement. O
    keep_table evita mesmo construir a string dos INSERT de tabelas que nao
    interessam -- num dump completo do Matomo isso e a maior parte do ficheiro
    (log_visit, archive_blob_*).
    """
    pending = []
    skipping = False
    in_string = False
    for line in fh:
        if not pending and not skipping:
            head = line[:400].lstrip()
            upper = head[:14].upper()
            if upper.startswith("INSERT INTO") or upper.startswith("REPLACE INTO"):
                if keep_table is not None:
                    found = _INSERT_TABLE_RE.match(head)
                    if found and not keep_table(found.group(1)):
                        skipping = True
            elif not upper.startswith("CREATE TABLE"):
                continue
        if not skipping:
            pending.append(line)
        in_string, ended = _scan_line(line, in_string)
        if ended:
            if pending:
                yield "".join(pending)
            pending = []
            skipping = False
            in_string = False
    if pending:
        yield "".join(pending)


def parse_create_table(statement):
    """Devolve (tabela, colunas) de um CREATE TABLE.

    A ordem das colunas e indispensavel: por omissao o mysqldump escreve
    "INSERT INTO t VALUES (...)" sem nomes de coluna, e sem o CREATE TABLE nao ha
    como saber a que coluna corresponde cada valor.
    """
    found = _CREATE_TABLE_RE.match(statement)
    if not found:
        return None, []
    columns = []
    for line in statement.splitlines():
        column = _COLUMN_DEF_RE.match(line)
        if column:
            columns.append(column.group(1))
    return found.group(1), columns


def parse_insert_header(statement):
    """Devolve (tabela, colunas ou None, indice onde a lista de valores comeca)."""
    found = _INSERT_HEADER_RE.match(statement)
    if not found:
        return None, None, 0
    columns = None
    if found.group(2):
        columns = [c.strip().strip("`") for c in found.group(2).split(",")]
    return found.group(1), columns, found.end()


def _read_string(text, pos):
    """Le um literal de texto a partir da aspa de abertura."""
    size = len(text)
    pos += 1
    out = []
    while pos < size:
        char = text[pos]
        if char == "\\":
            pos += 1
            if pos >= size:
                break
            out.append(_MYSQL_ESCAPES.get(text[pos], text[pos]))
            pos += 1
        elif char == "'":
            if pos + 1 < size and text[pos + 1] == "'":  # '' -> ' (dumps sem escapes)
                out.append("'")
                pos += 2
                continue
            return "".join(out), pos + 1
        else:
            out.append(char)
            pos += 1
    raise SystemExit("ERRO: literal de texto sem fecho no dump SQL.")


def _hex_literal(digits):
    """0x4142 / X'4142' -- aparece com mysqldump --hex-blob."""
    if len(digits) % 2:
        digits = "0" + digits
    try:
        return bytes.fromhex(digits).decode("utf-8", "replace")
    except ValueError:
        return digits


def _number(token):
    try:
        return int(token)
    except ValueError:
        try:
            return float(token)
        except ValueError:
            return token


def _read_value(text, pos):
    size = len(text)
    char = text[pos]
    if char == "'":
        return _read_string(text, pos)
    if char == "_":  # introdutor de charset: _binary'...', _utf8mb4'...'
        quote = text.find("'", pos)
        if quote < 0:
            raise SystemExit("ERRO: introdutor de charset sem literal no dump SQL.")
        return _read_string(text, quote)
    if char in "0123456789+-.":
        if text[pos:pos + 2].lower() == "0x":
            end = pos + 2
            while end < size and text[end] in "0123456789abcdefABCDEF":
                end += 1
            return _hex_literal(text[pos + 2:end]), end
        end = pos + 1
        while end < size and text[end] in _NUMBER_TAIL:
            end += 1
        return _number(text[pos:end]), end
    upper = text[pos:pos + 5].upper()
    if upper.startswith("NULL"):
        return None, pos + 4
    if upper.startswith("TRUE"):
        return 1, pos + 4
    if upper.startswith("FALSE"):
        return 0, pos + 5
    if upper.startswith("X'"):
        end = text.find("'", pos + 2)
        if end < 0:
            raise SystemExit("ERRO: literal hexadecimal sem fecho no dump SQL.")
        return _hex_literal(text[pos + 2:end]), end + 1
    raise SystemExit("ERRO: valor nao reconhecido no dump SQL: %r" % text[pos:pos + 40])


def _read_row(text, pos):
    """Le uma tupla (v1,v2,...) e devolve (valores, indice seguinte)."""
    size = len(text)
    pos += 1
    values = []
    while pos < size:
        while pos < size and text[pos] in " \t\r\n":
            pos += 1
        if pos < size and text[pos] == ")":
            return values, pos + 1
        value, pos = _read_value(text, pos)
        values.append(value)
        while pos < size and text[pos] in " \t\r\n":
            pos += 1
        if pos < size and text[pos] == ",":
            pos += 1
    raise SystemExit("ERRO: tupla de valores sem fecho no dump SQL.")


def iter_insert_rows(statement, pos):
    """Devolve as tuplas de um INSERT, uma a uma (extended insert incluido)."""
    size = len(statement)
    while pos < size:
        while pos < size and statement[pos] in " \t\r\n,":
            pos += 1
        if pos >= size or statement[pos] != "(":
            return
        row, pos = _read_row(statement, pos)
        yield row


# --------------------------------------------------------------------------- #
# Dump -> sqlite
# --------------------------------------------------------------------------- #

# So estas tabelas, e so estas colunas. Um dump completo do Matomo traz
# log_visit e archive_blob_* que nada disto usa e que dominam o tamanho.
DUMP_SCHEMA = {
    "log_link_visit_action": [
        ("idlink_va", "INTEGER PRIMARY KEY"),
        ("idvisit", "INTEGER"),
        ("idsite", "INTEGER"),
        ("server_time", "TEXT"),
        ("idpageview", "TEXT"),
        ("time_spent", "INTEGER"),
        ("idaction_url", "INTEGER"),
        ("idaction_url_ref", "INTEGER"),
        ("idaction_name", "INTEGER"),
        ("idaction_event_category", "INTEGER"),
        ("idaction_event_action", "INTEGER"),
        ("search_cat", "TEXT"),
        ("search_count", "INTEGER"),
    ],
    "log_action": [
        ("idaction", "INTEGER PRIMARY KEY"),
        ("name", "TEXT"),
        ("type", "INTEGER"),
        ("url_prefix", "INTEGER"),
    ],
    "site": [
        ("idsite", "INTEGER PRIMARY KEY"),
        ("name", "TEXT"),
        ("main_url", "TEXT"),
        ("ts_created", "TEXT"),
        ("timezone", "TEXT"),
    ],
    "option": [
        ("option_name", "TEXT PRIMARY KEY"),
        ("option_value", "TEXT"),
    ],
    "archive_numeric": [
        ("idsite", "INTEGER"),
        ("period", "INTEGER"),
        ("name", "TEXT"),
        ("date1", "TEXT"),
        ("value", "REAL"),
    ],
}

# Ordem de teste: as mais especificas primeiro, para "log_link_visit_action" nao
# ser confundida com "log_action".
DUMP_BASE_TABLES = ("log_link_visit_action", "log_action", "site", "option")
_ARCHIVE_NUMERIC_RE = re.compile(r"^(.*)archive_numeric_\d{4}_\d{1,2}$")

# Do matomo_option so interessam a versao e a configuracao de purga; o resto
# guarda blobs de configuracao que nao entram em nenhuma metrica.
_OPTION_KEEP_PREFIXES = ("version_core", "delete_logs", "delete_reports")


def _classify_dump_table(name, prefix_hint=None):
    """Devolve (base, prefixo) para as tabelas que interessam, senao (None, None).

    O prefixo (matomo_, piwik_, ...) e deduzido do proprio nome, para nao ser
    preciso saber a configuracao da instalacao de onde veio o dump.
    """
    for base in DUMP_BASE_TABLES:
        if name == base:
            prefix = ""
        elif name.endswith("_" + base):
            prefix = name[: -len(base)]
        else:
            continue
        if prefix_hint is not None and prefix != prefix_hint:
            continue
        return base, prefix
    found = _ARCHIVE_NUMERIC_RE.match(name)
    if found and (prefix_hint is None or found.group(1) == prefix_hint):
        return "archive_numeric", found.group(1)
    return None, None


def load_dump_into_sqlite(dump_path, sqlite_path, want_archive=False, prefix_hint=None,
                          batch_size=5000, progress_every=2000000):
    """Carrega o dump para sqlite e devolve (SqliteMatomoDatabase, contagens)."""
    if os.path.exists(sqlite_path):
        os.remove(sqlite_path)

    conn = sqlite3.connect(sqlite_path)
    # Base descartavel: sem journal nem fsync, o carregamento fica limitado pelo
    # gzip e nao pelo disco.
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA cache_size = -131072")  # 128 MiB

    tables = {}            # base -> nome real da tabela no dump
    archive_tables = []
    create_columns = {}    # nome real -> colunas, tal como no CREATE TABLE
    insert_sql = {}        # nome real -> INSERT preparado
    column_map = {}        # nome real -> indices no dump para cada coluna nossa
    counts = defaultdict(int)
    pending = defaultdict(list)
    next_report = defaultdict(lambda: progress_every)
    prefixes = {}
    statements = 0
    unmappable = []

    def base_of(name):
        return _classify_dump_table(name, prefix_hint)[0]

    def ensure_table(real_name, base, dump_cols):
        if real_name in insert_sql:
            return True
        schema = DUMP_SCHEMA[base]
        if not dump_cols:
            print(
                "  AVISO: %s aparece num INSERT sem lista de colunas e sem CREATE TABLE "
                "no dump -- ignorada." % real_name,
                file=sys.stderr,
            )
            unmappable.append(real_name)
            insert_sql[real_name] = None
            return False
        conn.execute('CREATE TABLE IF NOT EXISTS "%s" (%s)'
                     % (real_name, ", ".join("%s %s" % (c, t) for c, t in schema)))
        positions = {name: index for index, name in enumerate(dump_cols)}
        column_map[real_name] = [positions.get(column) for column, _ in schema]
        missing = [c for (c, _), p in zip(schema, column_map[real_name]) if p is None]
        if missing:
            # Versoes antigas do Matomo nao tem idpageview/time_spent; ficam NULL.
            print("  AVISO: %s nao tem as colunas %s -- ficam vazias."
                  % (real_name, ", ".join(missing)), file=sys.stderr)
        insert_sql[real_name] = 'INSERT OR REPLACE INTO "%s" VALUES (%s)' % (
            real_name, ", ".join("?" * len(schema)))
        if base == "archive_numeric":
            if real_name not in archive_tables:
                archive_tables.append(real_name)
        else:
            tables[base] = real_name
        return True

    def flush(real_name):
        rows = pending.pop(real_name, None)
        if rows:
            conn.executemany(insert_sql[real_name], rows)

    option_name_at = [c for c, _ in DUMP_SCHEMA["option"]].index("option_name")
    archive_period_at = [c for c, _ in DUMP_SCHEMA["archive_numeric"]].index("period")
    archive_name_at = [c for c, _ in DUMP_SCHEMA["archive_numeric"]].index("name")

    print("A ler %s (%.1f MiB) para %s ..."
          % (dump_path, os.path.getsize(dump_path) / (1024.0 * 1024.0), sqlite_path))

    def read_dump():
        """Percorre o dump. Isolado para transformar erros de descompressao a meio
        do ficheiro (backup truncado) num erro legivel em vez de um traceback."""
        with open_dump_text(dump_path) as fh:
            for statement in iter_dump_statements(fh, keep_table=lambda t: base_of(t) is not None):
                yield statement

    try:
        for statement in read_dump():
            statements += 1
            stripped = statement.lstrip()
            if stripped[:12].upper().startswith("CREATE TABLE"):
                real_name, columns = parse_create_table(statement)
                if real_name and columns:
                    create_columns[real_name] = columns
                    base, prefix = _classify_dump_table(real_name, prefix_hint)
                    if base is not None:
                        prefixes.setdefault(base, prefix)
                continue

            real_name, columns, pos = parse_insert_header(statement)
            if real_name is None:
                continue
            base, prefix = _classify_dump_table(real_name, prefix_hint)
            if base is None or (base == "archive_numeric" and not want_archive):
                continue
            prefixes.setdefault(base, prefix)
            if not ensure_table(real_name, base, columns or create_columns.get(real_name)):
                continue
            if insert_sql[real_name] is None:
                continue

            indices = column_map[real_name]
            bucket = pending[real_name]
            for row in iter_insert_rows(statement, pos):
                if not row:
                    continue
                size = len(row)
                values = [row[i] if i is not None and i < size else None for i in indices]
                if base == "option":
                    if not str(values[option_name_at] or "").startswith(_OPTION_KEEP_PREFIXES):
                        continue
                elif base == "archive_numeric":
                    if values[archive_period_at] != 1 or values[archive_name_at] != "nb_visits":
                        continue
                bucket.append(values)
                counts[base] += 1
                if len(bucket) >= batch_size:
                    flush(real_name)
                    bucket = pending[real_name]
            if counts[base] >= next_report[base]:
                print("    %-32s %12d linhas" % (real_name, counts[base]))
                while next_report[base] <= counts[base]:
                    next_report[base] += progress_every
    except (OSError, EOFError) as exc:
        # Um .gz/.bz2/.xz cortado a meio so rebenta aqui, depois de ja termos
        # carregado parte das linhas -- o que seria pior do que nao carregar nada.
        conn.close()
        raise SystemExit(
            "ERRO: %s parece truncado ou corrompido -- a leitura falhou depois de\n"
            "      %d statements (%s: %s). Copie o backup outra vez."
            % (dump_path, statements, type(exc).__name__, exc)
        )

    for real_name in list(pending):
        flush(real_name)
    conn.commit()

    if statements == 0:
        conn.close()
        raise SystemExit(
            "ERRO: %s nao tem um unico CREATE TABLE nem INSERT -- nao parece um dump\n"
            "      do mysqldump. Confirme o ficheiro (e a compressao, se tiver .gz)."
            % dump_path
        )

    if "log_link_visit_action" not in tables:
        conn.close()
        if unmappable:
            raise SystemExit(
                "ERRO: o dump traz dados (%s) mas nao a estrutura das tabelas, e os INSERT\n"
                "      nao nomeiam as colunas -- nao ha como saber a que coluna corresponde\n"
                "      cada valor. Refaca o dump com a estrutura (sem --no-create-info) ou\n"
                "      com --complete-insert." % ", ".join(sorted(set(unmappable)))
            )
        raise SystemExit(
            "ERRO: o dump nao tem linhas de <prefixo>log_link_visit_action.\n"
            "      Sem a tabela de acoes nao ha metricas a extrair. Se o dump usa um\n"
            "      prefixo pouco comum, indique-o com --sql-prefix."
        )
    if "log_action" not in tables:
        conn.close()
        raise SystemExit(
            "ERRO: o dump nao tem linhas de <prefixo>log_action -- e a tabela que\n"
            "      guarda os URLs, sem ela as acoes nao se conseguem interpretar."
        )

    distinct = sorted(set(prefixes.values()))
    prefix = prefixes["log_link_visit_action"]
    if len(distinct) > 1:
        print("  AVISO: o dump tem tabelas com mais do que um prefixo (%s); usado '%s'."
              % (", ".join(repr(p) for p in distinct), prefix), file=sys.stderr)

    print("  a indexar (idsite, server_time) ...")
    conn.execute('CREATE INDEX IF NOT EXISTS ix_llva_site_time ON "%s" (idsite, server_time)'
                 % tables["log_link_visit_action"])
    conn.execute("ANALYZE")
    conn.commit()

    print("  %d statements lidos; linhas carregadas: %s"
          % (statements, ", ".join("%s=%d" % (k, counts[k]) for k in sorted(counts)) or "nenhuma"))

    return SqliteMatomoDatabase(conn, tables, archive_tables, prefix), dict(counts)


# --------------------------------------------------------------------------- #
# Adaptador sqlite -> interface do driver MySQL
# --------------------------------------------------------------------------- #

def _translate_sql(sql):
    """Marcadores do pymysql (%s) para os do sqlite (?)."""
    return sql.replace("%s", "?").replace("%%", "%")


def _adapt_param(value):
    """Datas para o texto com que ficaram guardadas no sqlite.

    O formato 'YYYY-MM-DD HH:MM:SS' e o do proprio dump, e ordena-se
    lexicograficamente na mesma ordem que cronologicamente -- e o que faz os
    filtros por janela temporal do exportador funcionarem sem conversoes.
    """
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    return value


def _to_datetime(value):
    if not isinstance(value, str):
        return value
    try:
        return datetime.fromisoformat(value.replace(" ", "T", 1)[:26])
    except ValueError:
        return None  # datas zero do MySQL ('0000-00-00 00:00:00')


def _to_date(value):
    converted = _to_datetime(value)
    return converted.date() if isinstance(converted, datetime) else converted


# As funcoes do exportador tratam estas colunas como objectos de data, nao texto.
_ROW_CONVERTERS = {
    "server_time": _to_datetime,
    "ts_created": _to_datetime,
    "min_t": _to_datetime,
    "max_t": _to_datetime,
    "date1": _to_date,
    "date2": _to_date,
}


class _SqliteDictCursor:
    """Cursor sqlite com a interface que export_matomo_mariadb.query() usa.

    Devolve dicts e converte as colunas de tempo para datetime, para que as
    funcoes de extraccao vejam exactamente o que veem quando lem a MariaDB.
    """

    def __init__(self, connection):
        self._cursor = connection.cursor()
        self._keys = []
        self._converters = []

    def execute(self, sql, params=()):
        self._cursor.execute(_translate_sql(sql), tuple(_adapt_param(p) for p in params))
        self._keys = [column[0] for column in (self._cursor.description or [])]
        self._converters = [_ROW_CONVERTERS.get(key) for key in self._keys]
        return self

    def __iter__(self):
        keys, converters = self._keys, self._converters
        for row in self._cursor:
            yield {
                key: (convert(value) if convert is not None and value is not None else value)
                for key, convert, value in zip(keys, converters, row)
            }

    def fetchall(self):
        return list(self)

    def close(self):
        self._cursor.close()


class SqliteMatomoDatabase:
    """Ligacao minima, compativel com o exportador, sobre o sqlite do dump.

    O exportador chama conn.cursor(dictionary=True) para drivers que nao sejam
    pymysql/mysqlclient (export_matomo_mariadb.py:_dict_cursor), e e por essa
    porta que este adaptador entra sem precisar de alterar o exportador.
    """

    def __init__(self, connection, tables, archive_tables, prefix):
        self.raw = connection
        self.tables = tables
        self.archive_tables = archive_tables
        self.prefix = prefix

    def cursor(self, dictionary=False):
        return _SqliteDictCursor(self.raw)

    def rollback(self):
        pass  # so para completar a interface: nada aqui escreve na base do dump

    def close(self):
        self.raw.close()


# --------------------------------------------------------------------------- #
# Diagnostico do dump (mesmo formato do --info do exportador)
# --------------------------------------------------------------------------- #

def _iso_or_none(value):
    converted = _to_datetime(value)
    return converted.isoformat() if converted is not None else None


_EMPTY_COVERAGE = {
    "min_server_time_utc": None,
    "max_server_time_utc": None,
    "total_actions": 0,
    "total_visits": 0,
}


def collect_coverage_from_dump(db, header, dump_info):
    """Constroi o mesmo relatorio que o --info do exportador, medido no dump."""
    cursor = db.raw.cursor()
    info = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "site_id": None,
        "mariadb_version": header.get("server_version"),
        "matomo_version": None,
        "source": {"kind": "matomo-sql-dump", "table_prefix": db.prefix, "dump": dump_info},
        "actions_per_month": [],
        "actions_per_type": {},
    }

    options = {}
    if "option" in db.tables:
        cursor.execute('SELECT option_name, option_value FROM "%s"' % db.tables["option"])
        options = dict(cursor.fetchall())
    info["matomo_version"] = options.get("version_core")
    info["purge_settings"] = {
        name: value for name, value in sorted(options.items())
        if name.startswith("delete_logs") or name.startswith("delete_reports")
    }
    info["raw_data_purge_enabled"] = str(info["purge_settings"].get("delete_logs_enable", "0")) == "1"

    per_site = {}
    cursor.execute(
        'SELECT idsite, MIN(server_time), MAX(server_time), COUNT(*), COUNT(DISTINCT idvisit) '
        'FROM "%s" GROUP BY idsite ORDER BY idsite' % db.tables["log_link_visit_action"]
    )
    for idsite, min_t, max_t, actions, visits in cursor.fetchall():
        per_site[int(idsite)] = {
            "min_server_time_utc": _iso_or_none(min_t),
            "max_server_time_utc": _iso_or_none(max_t),
            "total_actions": int(actions or 0),
            "total_visits": int(visits or 0),
        }
    info["raw_log_coverage_per_site"] = per_site
    info["raw_log_coverage"] = dict(_EMPTY_COVERAGE)

    sites = []
    if "site" in db.tables:
        cursor.execute('SELECT idsite, name, main_url, ts_created, timezone FROM "%s" ORDER BY idsite'
                       % db.tables["site"])
        for idsite, name, main_url, ts_created, tz_name in cursor.fetchall():
            sites.append({
                "idsite": int(idsite),
                "name": name or "",
                "main_url": main_url,
                "ts_created": _iso_or_none(ts_created),
                "timezone": tz_name,
            })
    if not sites:
        # Dump parcial (sem a tabela site): o que se sabe dos sites vem dos logs.
        # Sem timezone o exportador agrupa os dias em UTC e diz que o fez.
        print("  AVISO: o dump nao tem a tabela <prefixo>site -- sem nome nem fuso horario\n"
              "         do site, os dias vao ser agrupados em UTC.", file=sys.stderr)
        sites = [
            {"idsite": idsite, "name": "(sem tabela site no dump)", "main_url": None,
             "ts_created": None, "timezone": None}
            for idsite in sorted(per_site)
        ]
    info["sites"] = sites

    info["archive_tables"] = {"numeric": sorted(db.archive_tables)} if db.archive_tables else {}
    return info


def finish_coverage_for_site(db, coverage, site_id):
    """Preenche a parte do relatorio que depende do site escolhido."""
    cursor = db.raw.cursor()
    llva = db.tables["log_link_visit_action"]
    coverage["site_id"] = site_id
    coverage["raw_log_coverage"] = coverage["raw_log_coverage_per_site"].get(site_id, dict(_EMPTY_COVERAGE))

    cursor.execute(
        'SELECT substr(server_time, 1, 7), COUNT(*), COUNT(DISTINCT idvisit) FROM "%s" '
        'WHERE idsite = ? GROUP BY 1 ORDER BY 1' % llva, (site_id,)
    )
    coverage["actions_per_month"] = [
        {"month": month, "actions": int(actions), "visits": int(visits)}
        for month, actions, visits in cursor.fetchall()
    ]

    cursor.execute(
        'SELECT a.type, COUNT(*) FROM "%s" llva JOIN "%s" a ON a.idaction = llva.idaction_url '
        'WHERE llva.idsite = ? GROUP BY a.type ORDER BY a.type'
        % (llva, db.tables["log_action"]), (site_id,)
    )
    coverage["actions_per_type"] = {str(kind): int(total) for kind, total in cursor.fetchall()}
    return coverage


def _describe_sites_with_logs(coverage):
    """Lista os sites com acoes, com nome e intervalo -- para escolher o --site-id.

    O nome importa: um backup de pre-producao tem os mesmos idsite que producao
    mas sites diferentes, e o numero sozinho nao denuncia a troca.
    """
    by_id = {site["idsite"]: site for site in coverage.get("sites", [])}
    lines = []
    for idsite, cov in sorted(coverage["raw_log_coverage_per_site"].items()):
        if not cov["total_actions"]:
            continue
        site = by_id.get(idsite, {})
        lines.append(
            "        --site-id %-4s %-24s %s  (%d acoes, %s a %s)"
            % (idsite, (site.get("name") or "?")[:24], (site.get("main_url") or "")[:34],
               cov["total_actions"],
               (cov.get("min_server_time_utc") or "?")[:10], (cov.get("max_server_time_utc") or "?")[:10])
        )
    return lines


def choose_site_from_dump(args, coverage):
    """Escolhe o idsite: o pedido, ou o unico com acoes no dump."""
    with_logs = sorted(
        idsite for idsite, cov in coverage["raw_log_coverage_per_site"].items() if cov["total_actions"]
    )
    listing = "\n".join(_describe_sites_with_logs(coverage))
    if args.site_id is not None:
        if args.site_id not in with_logs:
            raise SystemExit(
                "ERRO: o site %s nao tem acoes neste dump. Os que tem sao:\n%s"
                % (args.site_id, listing or "        (nenhum)")
            )
        return args.site_id
    if not with_logs:
        raise SystemExit("ERRO: o dump nao tem nenhuma acao em <prefixo>log_link_visit_action.")
    if len(with_logs) == 1:
        print("\nSite escolhido: idsite=%d (o unico com acoes no dump)." % with_logs[0])
        return with_logs[0]
    raise SystemExit(
        "ERRO: o dump tem acoes de %d sites. Indique qual com --site-id:\n%s"
        % (len(with_logs), listing)
    )


# --------------------------------------------------------------------------- #
# Dump -> export intermedio
# --------------------------------------------------------------------------- #

def _stamp_dump_source(export_dir, dump_info, prefix, row_counts):
    """Regista no manifesto que este export veio de um dump, nao da MariaDB.

    O manifest.json nao consta do proprio manifest["files"], logo reescreve-lo
    aqui nao invalida nenhum checksum.
    """
    path = os.path.join(export_dir, "manifest.json")
    with open(path, "r", encoding="utf-8") as fh:
        manifest = json.load(fh)
    manifest.setdefault("source", {}).update({
        "kind": "matomo-sql-dump",
        "driver": "sqlite",
        "table_prefix": prefix,
        "dump": dump_info,
        "dump_rows": row_counts,
    })
    manifest["built_from_sql_by"] = {
        "script": os.path.basename(__file__),
        "script_version": SCRIPT_VERSION,
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)


def import_from_sql_dump(args):
    """Constroi um export a partir de um dump SQL. Devolve (export_dir, a_limpar).

    Passar pelo formato de export nao e desperdicio: e o que garante manifesto,
    checksums e _id iguais aos do caminho normal, e deixa um artefacto
    inspeccionavel (--keep-staging) quando os numeros nao baterem certo.
    """
    exporter = load_exporter_module()

    if bool(args.date_from) != bool(args.date_to):
        raise SystemExit("ERRO: indique --date-from e --date-to em conjunto, ou nenhum (todo o dump).")

    # Ler o cabecalho antes de criar o staging: um dump ilegivel falha aqui, e
    # nesse caso nao chega a ficar nenhum directorio para tras.
    dump_path = os.path.abspath(args.export)
    header = read_dump_header(dump_path)

    if args.staging_dir:
        staging = os.path.abspath(args.staging_dir)
        os.makedirs(staging, exist_ok=True)
        temporary = False
    else:
        staging = tempfile.mkdtemp(prefix="matomo_sql_import_")
        temporary = True

    dump_info = {
        "path": dump_path,
        "bytes": os.path.getsize(dump_path),
        "modified_at": datetime.fromtimestamp(os.path.getmtime(dump_path), timezone.utc).isoformat(),
    }
    dump_info.update(header)
    if header.get("dump_tool"):
        print("Dump: %s" % header["dump_tool"])

    sqlite_path = os.path.join(staging, "matomo_dump.sqlite")
    try:
        db, row_counts = load_dump_into_sqlite(
            dump_path, sqlite_path,
            want_archive=args.verify_archive,
            prefix_hint=args.sql_prefix,
        )
    except BaseException:
        # Sem isto um dump ilegivel deixava um directorio temporario para tras.
        if temporary:
            shutil.rmtree(staging, ignore_errors=True)
        raise

    try:
        coverage = collect_coverage_from_dump(db, header, dump_info)
        site_id = choose_site_from_dump(args, coverage)
        finish_coverage_for_site(db, coverage, site_id)
        print()
        exporter.print_coverage(coverage)

        export_args = argparse.Namespace(
            site_id=site_id,
            date_from=args.date_from,
            date_to=args.date_to,
            all=not args.date_from,
            out_dir=os.path.join(staging, "export"),
            tar=False,
            # Com --only agg nem vale a pena escrever as acoes em bruto: nao seriam
            # importadas e sao a maior parte do volume.
            no_raw=(args.only == "agg"),
            raw_all_actions=args.raw_all_actions,
            no_sql_prefilter=args.no_sql_prefilter,
            verify_archive=args.verify_archive,
            overwrite=True,
            utc_days=args.utc_days,
        )
        export_dir = exporter.run_export(db, "sqlite", db.prefix, export_args, coverage)
    except BaseException:
        db.close()
        if temporary:
            shutil.rmtree(staging, ignore_errors=True)
        raise
    else:
        db.close()

    _stamp_dump_source(export_dir, dump_info, db.prefix, row_counts)

    cleanup = []
    if args.keep_staging:
        print("\nStaging mantido (--keep-staging): %s" % staging)
    else:
        cleanup.append(sqlite_path)
        if temporary:
            cleanup = [staging]
        else:
            print("\nExport intermedio mantido em %s" % export_dir)
    return export_dir, cleanup


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
    parser.add_argument("export", help="directorio do export, .tar.gz, ou dump .sql[.gz]")
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

    sql = parser.add_argument_group(
        "entrada .sql[.gz]",
        "opcoes usadas so quando a entrada e um dump do mysqldump (.sql, .sql.gz, "
        ".sql.bz2, .sql.xz) em vez de um export",
    )
    sql.add_argument("--from-sql", action="store_true",
                     help="forca tratar a entrada como dump SQL (extensao pouco comum)")
    sql.add_argument("--site-id", type=int,
                     help="idsite a extrair; por omissao o unico site com acoes no dump")
    sql.add_argument("--date-from", help="primeiro dia a extrair (YYYY-MM-DD); por omissao todo o dump")
    sql.add_argument("--date-to", help="ultimo dia a extrair (YYYY-MM-DD)")
    sql.add_argument("--sql-prefix", help="prefixo das tabelas (def. deduzido do dump: matomo_, piwik_, ...)")
    sql.add_argument("--utc-days", action="store_true",
                     help="agrupa os dias em UTC em vez do fuso do site")
    sql.add_argument("--raw-all-actions", action="store_true",
                     help="guarda todas as acoes, nao so as que casam com os padroes de URL")
    sql.add_argument("--no-sql-prefilter", action="store_true",
                     help="nao filtra os URLs por LIKE (mais lento, util para depurar)")
    sql.add_argument("--verify-archive", action="store_true",
                     help="compara nb_visits diario com as tabelas archive_numeric_* do dump")
    sql.add_argument("--staging-dir",
                     help="onde criar o sqlite e o export intermedios (def. directorio temporario). "
                          "Conta com espaco da ordem do dump descomprimido")
    sql.add_argument("--keep-staging", action="store_true",
                     help="nao apaga o sqlite nem o export intermedios")
    args = parser.parse_args()

    from_sql = args.from_sql or looks_like_sql_dump(args.export)
    if not from_sql:
        ignored = [
            name for name, value in (
                ("--site-id", args.site_id), ("--date-from", args.date_from), ("--date-to", args.date_to),
                ("--sql-prefix", args.sql_prefix), ("--utc-days", args.utc_days),
                ("--raw-all-actions", args.raw_all_actions), ("--no-sql-prefilter", args.no_sql_prefilter),
                ("--verify-archive", args.verify_archive), ("--staging-dir", args.staging_dir),
                ("--keep-staging", args.keep_staging),
            ) if value
        ]
        if ignored:
            print("AVISO: %s so se aplica(m) a uma entrada .sql[.gz]; ignorado(s) -- %s e um export."
                  % (", ".join(ignored), args.export), file=sys.stderr)

    if from_sql:
        export_dir, cleanup = import_from_sql_dump(args)
    else:
        export_dir, tmp_dir = open_export(args.export)
        cleanup = [tmp_dir] if tmp_dir else []
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
                    "source": manifest.get("source"),
                },
                "only": args.only,
                "collections": {k: dict(v) for k, v in totals.items()},
                "resolution": resolution,
            })

        client.close()
        print("\nConcluido%s." % (" (dry-run)" if args.dry_run else ""))
        return 0
    finally:
        for path in cleanup:
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            elif os.path.exists(path):
                os.remove(path)


if __name__ == "__main__":
    sys.exit(main())
