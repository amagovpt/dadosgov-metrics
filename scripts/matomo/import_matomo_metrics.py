#!/usr/bin/env python3
"""
Importa as metricas do Matomo para as bases de dados de destino a partir do dump SQL.

Le o ficheiro metrics-full.sql (produzido a partir do Matomo pela cadeia
export_matomo_mariadb.py -> ficheiros) e escreve-o nas duas bases de dados que
guardam metricas:

  - PostgreSQL, schema `metric` -- por omissao a base de dados CSV do hydra
    (container hydra-pt-database-csv-1, porta 5434), que a metrics-api/api-tabular
    le. Aplica o SQL do ficheiro tal e qual.
  - MongoDB, coleccao `metrics` do udata -- a serie diaria por objecto, na forma
    {object_id, date, level: "daily", values: {nb_visits}}, que e a que la esta
    desde a importacao Matomo de 2021-2023. So escreve `values.nb_visits`; as
    outras chaves de `values` pertencem ao udata e ao DAG metrics_etl.

Substitui a extraccao via Reporting API HTTP que este script fazia antes: o
endpoint http://10.50.37.53/stats/ deixou de estar disponivel na intranet. Ja nao
e preciso resolver slugs contra o MongoDB -- o dump traz os ObjectIds resolvidos.

Do lado do PostgreSQL o ficheiro e auto-suficiente e idempotente:
  - CREATE SCHEMA/TABLE/UNIQUE INDEX ... IF NOT EXISTS (nao altera o que ja existe;
    as tabelas de producao tem colunas a mais -- __id, nb_visit_* -- e ficam intactas)
  - INSERT ... ON CONFLICT (<objecto>_id, date_metric) DO UPDATE SET nb_visit = EXCLUDED.nb_visit
  - bloco DO final que refresca as materialized views de `metric` que existirem

Porque nao um simples `psql -f`: aqui ha filtros por data e por tabela, resumo do
conteudo do ficheiro sem tocar na base de dados (--info), simulacao (--dry-run),
varios destinos na mesma passagem, contagens antes/depois e uma transaccao gerida
pelo script (o BEGIN/COMMIT do ficheiro e ignorado).

Uso:
    # o que esta no ficheiro (nao liga a nenhuma base de dados)
    python3 import_matomo_metrics.py --info

    # simulacao: plano + estado actual dos destinos, sem escrever nada
    python3 import_matomo_metrics.py --dry-run

    # importacao completa: PostgreSQL (127.0.0.1:5434) + MongoDB (udata.metrics)
    python3 import_matomo_metrics.py

    # so uma das bases de dados
    python3 import_matomo_metrics.py --skip-mongo
    python3 import_matomo_metrics.py --skip-postgres

    # so um intervalo de datas e so algumas tabelas, sem refrescar as matviews
    python3 import_matomo_metrics.py --date-from 2026-01-01 --date-to 2026-08-04 \
        --tables datasets,resources --no-refresh

    # outro ambiente: .env proprio (ou variaveis exportadas)
    python3 import_matomo_metrics.py --env-file /etc/dadosgov/qa.env

    # varios PostgreSQL na mesma passagem
    python3 import_matomo_metrics.py \
        --target "host=127.0.0.1 port=5434 dbname=postgres user=postgres password=postgres" \
        --target "postgresql://utilizador:password@outro-host:5434/postgres"

Configuracao. Le-se, por esta ordem: argumentos > ambiente exportado > ficheiro
.env (o que estiver ao lado do script, ou o de --env-file) > valores por omissao.

    # PostgreSQL -- corre sempre na mesma maquina que o script, por isso tem omissoes
    METRICS_PG_DSN       DSN completo (libpq ou URL); tem prioridade sobre as seguintes
    METRICS_PG_HOST      (def. 127.0.0.1)    METRICS_PG_PORT      (def. 5434)
    METRICS_PG_DB        (def. postgres)     METRICS_PG_USER      (def. postgres)
    METRICS_PG_PASSWORD  (def. postgres)

    # MongoDB -- noutra VM, diferente em cada ambiente: NAO tem valor por omissao.
    # Sem um destes o script para e explica; --skip-mongo salta o MongoDB.
    METRICS_MONGO_URI    URI completo (mongodb://[user:password@]host:porta)
    MONGODB_HOST         host                MONGODB_PORT         (def. 27017)
    METRICS_MONGO_DB     (def. udata)        METRICS_MONGO_COLLECTION (def. metrics)

Requisitos: Python 3.9+, psycopg2 e pymongo
(pip install psycopg2-binary pymongo).
"""

import argparse
import bz2
import gzip
import lzma
import os
import re
import sys
import time
from datetime import datetime

try:
    import psycopg2
    from psycopg2 import extensions as pg_ext
except ImportError:
    sys.stderr.write("ERRO: falta o psycopg2. Instale com: pip install psycopg2-binary\n")
    raise SystemExit(2)

SCRIPT_VERSION = "2.0.0"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SQL_FILE = os.path.join(SCRIPT_DIR, "metrics-full.sql")
DEFAULT_ENV_FILE = os.path.join(SCRIPT_DIR, ".env")

# Tabelas base do schema metric que o dump preenche, pela ordem dos relatorios.
METRIC_TABLES = (
    "visits_datasets",
    "visits_resources",
    "visits_reuses",
    "visits_organizations",
    "visits_dataservices",
)

class ImportFailed(RuntimeError):
    """Erro ao aplicar o dump a um destino."""


# --------------------------------------------------------------------------- #
# Configuracao (ficheiro .env + ambiente)
# --------------------------------------------------------------------------- #
#
# O PostgreSQL corre sempre na mesma maquina que este script, por isso tem
# valores por omissao (a BD CSV do hydra, em 127.0.0.1:5434). O MongoDB vive
# noutra VM, diferente em cada ambiente: nao tem omissao nenhuma e tem de vir
# de --mongo-uri, do ambiente ou do .env, senao o script para e diz como se
# configura.
#
# Precedencia: argumentos > ambiente ja exportado > ficheiro .env > omissoes.

def default_pg():
    """Destino PostgreSQL por omissao: local, onde o schema metric vive."""
    return {
        "host": os.environ.get("METRICS_PG_HOST", "127.0.0.1"),
        "port": os.environ.get("METRICS_PG_PORT", "5434"),
        "dbname": os.environ.get("METRICS_PG_DB", "postgres"),
        "user": os.environ.get("METRICS_PG_USER", "postgres"),
        "password": os.environ.get("METRICS_PG_PASSWORD", "postgres"),
    }


def load_env_file(path, required=False):
    """
    Carrega KEY=VALUE de um ficheiro .env para o ambiente do processo.

    Nao sobrepoe o que ja estiver exportado -- o ambiente real e os argumentos
    ganham sempre ao ficheiro. Sem python-dotenv de proposito: e uma dependencia
    a mais para um formato de duas linhas, e os outros scripts desta pasta
    tambem nao a têm.
    """
    if not os.path.exists(path):
        if required:
            raise SystemExit("ERRO: ficheiro indicado em --env-file nao existe: %s" % path)
        return 0

    loaded = 0
    with open(path, "r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export "):].lstrip()
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            if key and key not in os.environ:
                os.environ[key] = value
                loaded += 1
    return loaded


def bootstrap_env(argv):
    """
    Le o .env antes de o argparse construir os valores por omissao.

    Sem --env-file usa o .env ao lado do script, se existir; cada ambiente tem
    o seu (o ficheiro esta no .gitignore). Devolve (caminho, nº de variaveis).
    """
    path = None
    explicit = False
    for index, arg in enumerate(argv):
        if arg == "--env-file" and index + 1 < len(argv):
            path, explicit = argv[index + 1], True
        elif arg.startswith("--env-file="):
            path, explicit = arg.split("=", 1)[1], True
    if path is None:
        path = DEFAULT_ENV_FILE
    return path, load_env_file(path, required=explicit)


# --------------------------------------------------------------------------- #
# Leitura e analise do ficheiro SQL
# --------------------------------------------------------------------------- #

# Tokens que mudam o estado do analisador quando estamos fora de string/comentario.
_TOKEN_RE = re.compile(r"'|--|/\*|;|\$[A-Za-z_0-9]*\$")
# Marcadores de dia que o exportador escreve antes dos INSERTs: "-- 2026-03-23: 41 row(s)".
_DAY_RE = re.compile(r"^--\s*(\d{4}-\d{2}-\d{2})\s*:\s*(\d+)\s+row", re.I)
_INSERT_RE = re.compile(r"^INSERT\s+INTO\s+([\w.\"]+)", re.I)
_INSERT_PARTS_RE = re.compile(
    r"^INSERT\s+INTO\s+(?P<table>[\w.\"]+)\s*\((?P<cols>[^)]*)\)\s*VALUES\s*(?P<rest>.*)$", re.I | re.S)
_CONFLICT_RE = re.compile(r"\bON\s+CONFLICT\s*\((?P<cols>[^)]*)\)", re.I)
_EXCLUDED_RE = re.compile(r"(\w+)\s*=\s*EXCLUDED\.", re.I)
_TX_RE = re.compile(r"^(BEGIN|COMMIT|END|ROLLBACK|START\s+TRANSACTION)\b", re.I)
_DDL_RE = re.compile(r"^(CREATE|ALTER|DROP|COMMENT|GRANT|REVOKE|TRUNCATE)\b", re.I)
_DO_RE = re.compile(r"^DO\b", re.I)
# No dump cada tuplo de valores ocupa uma linha propria: conta as linhas de um INSERT.
_ROW_RE = re.compile(r"^[ \t]*\(", re.M)


class Statement(object):
    """Uma instrucao SQL do dump, ja classificada."""

    __slots__ = ("sql", "day", "line", "kind", "table", "rows")

    def __init__(self, sql, day, line):
        self.sql = sql
        self.day = day
        self.line = line
        self.table = None
        self.rows = 0

        match = _INSERT_RE.match(sql)
        if match:
            self.kind = "insert"
            self.table = match.group(1).replace('"', "").lower()
            self.rows = len(_ROW_RE.findall(sql))
        elif _TX_RE.match(sql):
            self.kind = "tx"
        elif _DO_RE.match(sql):
            # O unico bloco DO do dump e o que refresca as materialized views.
            self.kind = "refresh"
        elif _DDL_RE.match(sql):
            self.kind = "ddl"
        else:
            self.kind = "other"

    @property
    def short_table(self):
        return self.table.rsplit(".", 1)[-1] if self.table else None

    def head(self, width=160):
        flat = " ".join(self.sql.split())
        return flat if len(flat) <= width else flat[:width] + "..."


def open_sql(path):
    """Abre o dump; aceita texto simples ou comprimido (.gz/.bz2/.xz)."""
    lowered = path.lower()
    if lowered.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    if lowered.endswith(".bz2"):
        return bz2.open(path, "rt", encoding="utf-8", errors="replace")
    if lowered.endswith(".xz"):
        return lzma.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def iter_statements(handle):
    """
    Gera Statement() por cada instrucao do ficheiro, sem o carregar todo em memoria.

    Percorre linha a linha com um analisador que conhece strings ('...' com ''
    escapado), comentarios de linha (--) e de bloco e blocos dollar-quoted ($$),
    para que o ; dentro do bloco DO final nao parta a instrucao ao meio. Os
    comentarios "-- YYYY-MM-DD: N row(s)" que precedem cada bloco diario ficam
    guardados em Statement.day, que e o que permite filtrar por data.
    """
    buf = []
    day = None
    stmt_line = None
    in_string = False
    dollar_tag = None
    in_block_comment = False

    for lineno, line in enumerate(handle, 1):
        pos = 0
        length = len(line)
        while pos < length:
            if dollar_tag is not None:
                idx = line.find(dollar_tag, pos)
                if idx < 0:
                    buf.append(line[pos:])
                    pos = length
                else:
                    end = idx + len(dollar_tag)
                    buf.append(line[pos:end])
                    pos = end
                    dollar_tag = None
                continue

            if in_string:
                idx = line.find("'", pos)
                if idx < 0:
                    buf.append(line[pos:])
                    pos = length
                elif line[idx + 1:idx + 2] == "'":
                    buf.append(line[pos:idx + 2])  # aspa escapada, a string continua
                    pos = idx + 2
                else:
                    buf.append(line[pos:idx + 1])
                    pos = idx + 1
                    in_string = False
                continue

            if in_block_comment:
                idx = line.find("*/", pos)
                if idx < 0:
                    pos = length
                else:
                    pos = idx + 2
                    in_block_comment = False
                continue

            match = _TOKEN_RE.search(line, pos)
            if match is None:
                chunk = line[pos:]
                if stmt_line is None and chunk.strip():
                    stmt_line = lineno
                buf.append(chunk)
                break

            chunk = line[pos:match.start()]
            if stmt_line is None and chunk.strip():
                stmt_line = lineno
            buf.append(chunk)
            token = match.group(0)
            pos = match.end()

            if token == "'":
                if stmt_line is None:
                    stmt_line = lineno
                buf.append("'")
                in_string = True
            elif token == "--":
                # Comentario ate ao fim da linha; so interessa o marcador do dia,
                # e apenas quando aparece entre instrucoes.
                if not "".join(buf).strip():
                    day_match = _DAY_RE.match(line[match.start():].strip())
                    if day_match:
                        day = day_match.group(1)
                pos = length
            elif token == "/*":
                in_block_comment = True
            elif token == ";":
                sql = "".join(buf).strip()
                buf = []
                if sql:
                    yield Statement(sql, day, stmt_line or lineno)
                stmt_line = None
            else:  # abertura de bloco dollar-quoted ($$ ou $tag$)
                if stmt_line is None:
                    stmt_line = lineno
                buf.append(token)
                dollar_tag = token

    tail = "".join(buf).strip()
    if tail:
        # Instrucao sem ; final: aplica-se na mesma, mas convem saber.
        sys.stderr.write("AVISO: o ficheiro termina com uma instrucao sem ';' final.\n")
        yield Statement(tail, day, stmt_line or 0)


def read_dump_header(path, max_lines=8):
    """Devolve os comentarios iniciais do dump (cabecalho escrito pelo exportador)."""
    header = []
    with open_sql(path) as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                break
            if not stripped.startswith("--"):
                break
            header.append(stripped.lstrip("-").strip())
            if len(header) >= max_lines:
                break
    return header


# --------------------------------------------------------------------------- #
# Linhas repetidas dentro do mesmo INSERT
# --------------------------------------------------------------------------- #

class DuplicateReport(object):
    """Contabiliza as linhas repetidas encontradas e resolvidas nos INSERTs."""

    __slots__ = ("rows", "statements", "examples")

    MAX_EXAMPLES = 5

    def __init__(self):
        self.rows = 0
        self.statements = 0
        self.examples = []

    def add(self, table, day, obj, kept, dropped):
        self.rows += 1
        if len(self.examples) < self.MAX_EXAMPLES:
            self.examples.append((table, day, obj, kept, dropped))

    def describe(self, mode):
        what = {"sum": "somadas", "first": "ficou a primeira", "last": "ficou a ultima"}[mode]
        lines = ["%s linhas repetidas em %s instrucoes (mesmo objecto no mesmo dia): %s"
                 % (fmt_int(self.rows), fmt_int(self.statements), what)]
        for table, day, obj, kept, dropped in self.examples:
            lines.append("      %s %s %s: %s + %s" % (table, day, obj, kept, dropped))
        if self.rows > len(self.examples):
            lines.append("      (... %s outras)" % fmt_int(self.rows - len(self.examples)))
        return lines


def _split_fields(text):
    """Divide um tuplo de VALUES nos seus campos, respeitando aspas e parentesis."""
    fields = []
    depth = 0
    in_string = False
    start = 0
    i = 0
    size = len(text)
    while i < size:
        char = text[i]
        if in_string:
            if char == "'":
                if text[i + 1:i + 2] == "'":
                    i += 2
                    continue
                in_string = False
        elif char == "'":
            in_string = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            fields.append(text[start:i].strip())
            start = i + 1
        i += 1
    fields.append(text[start:].strip())
    return fields


def _split_value_tuples(text):
    """Separa os tuplos de VALUES do resto da instrucao (a clausula ON CONFLICT)."""
    tuples = []
    i = 0
    size = len(text)
    while i < size:
        char = text[i]
        if char.isspace() or char == ",":
            i += 1
            continue
        if char != "(":
            break  # acabaram os valores; daqui para a frente e o ON CONFLICT
        depth = 0
        in_string = False
        j = i
        while j < size:
            inner = text[j]
            if in_string:
                if inner == "'":
                    if text[j + 1:j + 2] == "'":
                        j += 2
                        continue
                    in_string = False
            elif inner == "'":
                in_string = True
            elif inner == "(":
                depth += 1
            elif inner == ")":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        tuples.append(text[i + 1:j])
        i = j + 1
    return tuples, text[i:]


def parse_insert(sql):
    """Parte um INSERT em (tabela, colunas, linhas, resto). None se nao encaixar no formato."""
    parts = _INSERT_PARTS_RE.match(sql)
    if parts is None:
        return None
    columns = [c.strip().strip('"').lower() for c in parts.group("cols").split(",")]
    tuples, tail = _split_value_tuples(parts.group("rest"))
    rows = []
    for raw in tuples:
        fields = _split_fields(raw)
        if len(fields) != len(columns):
            return None  # layout inesperado: quem chama que decida o que fazer
        rows.append(fields)
    return parts.group("table"), columns, rows, tail


def unquote(literal):
    """Literal SQL -> valor Python (str, int ou None)."""
    if literal is None:
        return None
    text = literal.strip()
    if text.upper() == "NULL":
        return None
    if len(text) >= 2 and text[0] == "'" and text[-1] == "'":
        return text[1:-1].replace("''", "'")
    try:
        return int(text)
    except ValueError:
        return text


def _merge_rows(kept, extra, columns, key_idx, sum_cols):
    """Junta duas linhas com a mesma chave: soma os contadores, mantem o resto."""
    merged = list(kept)
    for pos, name in enumerate(columns):
        if pos in key_idx:
            continue
        if name in sum_cols:
            # Contador: soma-se sempre, mesmo que as duas linhas tragam o mesmo valor.
            try:
                merged[pos] = str(int(merged[pos]) + int(extra[pos]))
            except ValueError:
                pass  # nao e inteiro (NULL, expressao): fica o primeiro valor
            continue
        if merged[pos] != extra[pos] and merged[pos].upper() == "NULL":
            merged[pos] = extra[pos]
    return merged


def dedupe_insert(stmt, mode, report):
    """
    Resolve as linhas repetidas dentro de um mesmo INSERT.

    O PostgreSQL rejeita a instrucao inteira ("ON CONFLICT DO UPDATE command cannot
    affect row a second time") quando o mesmo par (objecto, dia) aparece duas vezes
    nos mesmos VALUES. Acontece 51 vezes no metrics-full.sql -- o mesmo objecto foi
    contado em dois URLs no mesmo dia -- e e o que faz um `psql -f` abortar a meio.
    Por omissao somam-se as visitas (sao contagens parciais do mesmo dia); --on-duplicate
    permite ficar com a primeira/ultima, ou deixar o erro rebentar como no psql.
    """
    if mode == "error":
        return stmt

    parsed = parse_insert(stmt.sql)
    if parsed is None:
        return stmt
    table, columns, rows, tail = parsed
    if len(rows) < 2:
        return stmt
    conflict = _CONFLICT_RE.search(tail)
    if conflict is None:
        return stmt  # sem ON CONFLICT nao ha restricao a violar

    key_cols = [c.strip().strip('"').lower() for c in conflict.group("cols").split(",")]
    try:
        key_idx = [columns.index(col) for col in key_cols]
    except ValueError:
        return stmt  # a chave usa uma expressao que nao sabemos mapear: deixa ao PostgreSQL
    # As colunas que o DO UPDATE substitui sao contadores -> sao as que se somam.
    sum_cols = {name.lower() for name in _EXCLUDED_RE.findall(tail)}

    seen = {}
    kept_rows = []
    duplicates = 0
    for fields in rows:
        key = tuple(fields[i] for i in key_idx)
        pos = seen.get(key)
        if pos is None:
            seen[key] = len(kept_rows)
            kept_rows.append(fields)
            continue

        duplicates += 1
        if report is not None:
            # So o identificador do objecto e os contadores: a data ja vai a parte.
            obj = "/".join(value.strip("'") for value in key if value.strip("'") != stmt.day)
            counters = [i for i, name in enumerate(columns) if name in sum_cols] or [len(columns) - 1]
            report.add(stmt.short_table, stmt.day, obj or "?",
                       "/".join(kept_rows[pos][i] for i in counters),
                       "/".join(fields[i] for i in counters))
        if mode == "last":
            kept_rows[pos] = fields
        elif mode == "sum":
            kept_rows[pos] = _merge_rows(kept_rows[pos], fields, columns, key_idx, sum_cols)
        # mode == "first": nao ha nada a fazer

    if not duplicates:
        return stmt
    if report is not None:
        report.statements += 1

    body = ",\n    ".join("(%s)" % ", ".join(fields) for fields in kept_rows)
    rebuilt = "INSERT INTO %s (%s) VALUES\n    %s\n%s" % (
        table, ", ".join(columns), body, tail.strip())
    return Statement(rebuilt, stmt.day, stmt.line)


# --------------------------------------------------------------------------- #

def iter_plan(path, args, report=None):
    """Gera as instrucoes a aplicar, ja filtradas pelas opcoes da linha de comandos."""
    with open_sql(path) as handle:
        for stmt in iter_statements(handle):
            if stmt.kind == "tx":
                continue  # a transaccao e gerida aqui, nao pelo ficheiro
            if stmt.kind == "ddl" and args.skip_ddl:
                continue
            if stmt.kind == "refresh" and args.no_refresh:
                continue
            if stmt.kind == "insert":
                if args.date_from and (stmt.day is None or stmt.day < args.date_from):
                    continue
                if args.date_to and (stmt.day is None or stmt.day > args.date_to):
                    continue
                if args.tables and stmt.short_table not in args.tables:
                    continue
                stmt = dedupe_insert(stmt, args.on_duplicate, report)
            yield stmt


def summarise(path, args):
    """Uma passagem pelo ficheiro para saber o que vai ser aplicado."""
    plan = {
        "ddl": 0,
        "other": 0,
        "refresh": 0,
        "inserts": 0,
        "rows": 0,
        "tables": {},
        "days": set(),
        "duplicates": DuplicateReport(),
    }
    for stmt in iter_plan(path, args, plan["duplicates"]):
        if stmt.kind == "insert":
            entry = plan["tables"].setdefault(stmt.table, {"stmts": 0, "rows": 0})
            entry["stmts"] += 1
            entry["rows"] += stmt.rows
            plan["inserts"] += 1
            plan["rows"] += stmt.rows
            if stmt.day:
                plan["days"].add(stmt.day)
        elif stmt.kind in ("ddl", "refresh", "other"):
            plan[stmt.kind] += 1
    return plan


# --------------------------------------------------------------------------- #
# Formatacao
# --------------------------------------------------------------------------- #

def fmt_int(value):
    return "{:,}".format(value).replace(",", " ")


def fmt_size(num_bytes):
    for unit in ("B", "KiB", "MiB", "GiB"):
        if num_bytes < 1024 or unit == "GiB":
            return "%.1f %s" % (num_bytes, unit) if unit != "B" else "%d B" % num_bytes
        num_bytes /= 1024.0


def fmt_date(value):
    return value.isoformat() if hasattr(value, "isoformat") else ("-" if value is None else str(value))


def table_order(name):
    short = name.rsplit(".", 1)[-1]
    return (METRIC_TABLES.index(short) if short in METRIC_TABLES else len(METRIC_TABLES), name)


def print_plan(plan, args):
    print("\nPlano%s:" % (" (com filtros)" if (args.date_from or args.date_to or args.tables
                                               or args.skip_ddl or args.no_refresh) else ""))
    print("  instrucoes DDL       : %s" % fmt_int(plan["ddl"]))
    print("  INSERTs              : %s  ->  %s linhas" % (fmt_int(plan["inserts"]), fmt_int(plan["rows"])))
    for table in sorted(plan["tables"], key=table_order):
        entry = plan["tables"][table]
        print("      %-32s %8s instrucoes  %10s linhas"
              % (table, fmt_int(entry["stmts"]), fmt_int(entry["rows"])))
    if plan["days"]:
        days = sorted(plan["days"])
        print("  dias com dados       : %s  (%s .. %s)" % (fmt_int(len(days)), days[0], days[-1]))
    else:
        print("  dias com dados       : 0")
    if plan["other"]:
        print("  outras instrucoes    : %s" % fmt_int(plan["other"]))
    print("  refresh das matviews : %s" % ("sim" if plan["refresh"] else "nao"))
    duplicates = plan["duplicates"]
    if duplicates.rows:
        for line in duplicates.describe(args.on_duplicate):
            print("  %s" % line)


def print_snapshot(snap, title):
    print("  %s:" % title)
    if not snap:
        print("      (o schema metric ainda nao tem tabelas neste destino)")
        return
    for table in sorted(snap, key=table_order):
        count, first, last = snap[table]
        print("      %-24s %10s linhas   %s .. %s"
              % (table, fmt_int(count), fmt_date(first), fmt_date(last)))


def print_snapshot_diff(before, after):
    print("  Resultado:")
    tables = sorted(set(before) | set(after), key=table_order)
    if not tables:
        print("      (sem tabelas em metric)")
        return
    print("      %-24s %12s %12s %12s   %s" % ("tabela", "antes", "depois", "novas", "intervalo"))
    for table in tables:
        old = before.get(table, (0, None, None))
        new = after.get(table, (0, None, None))
        delta = new[0] - old[0]
        print("      %-24s %12s %12s %12s   %s .. %s"
              % (table, fmt_int(old[0]), fmt_int(new[0]),
                 ("+" + fmt_int(delta)) if delta else "0",
                 fmt_date(new[1]), fmt_date(new[2])))


# --------------------------------------------------------------------------- #
# Destinos PostgreSQL
# --------------------------------------------------------------------------- #

def resolve_targets(args):
    """Lista de DSNs de destino: --target, senao METRICS_PG_DSN, senao o default."""
    if args.target:
        return list(args.target)
    env_dsn = os.environ.get("METRICS_PG_DSN")
    if env_dsn:
        return [env_dsn]
    return [pg_ext.make_dsn(**default_pg())]


def dsn_label(dsn):
    """Etiqueta legivel do destino, sempre sem a password."""
    try:
        info = pg_ext.parse_dsn(dsn)
    except Exception:
        return "destino"
    return "%s@%s:%s/%s" % (info.get("user", "?"), info.get("host", "?"),
                            info.get("port", "5432"), info.get("dbname", "?"))


def existing_metric_tables(conn):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.relname
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'metric' AND c.relkind = 'r'
        """
    )
    names = {row[0] for row in cur.fetchall()}
    cur.close()
    return names


def snapshot(conn):
    """count(*)/min/max de date_metric por tabela base de metric (so as que existem)."""
    present = existing_metric_tables(conn)
    result = {}
    cur = conn.cursor()
    for table in METRIC_TABLES:
        if table not in present:
            continue
        # Nome vindo de METRIC_TABLES (constante do script), nunca do ficheiro.
        cur.execute("SELECT count(*), min(date_metric), max(date_metric) FROM metric.%s" % table)
        result[table] = cur.fetchone()
    cur.close()
    return result


def import_into(dsn, args):
    """Aplica o dump a um destino. Devolve (instrucoes, linhas) aplicadas."""
    label = dsn_label(dsn)
    print("\n=== %s ===" % label)

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        before = snapshot(conn)
        print_snapshot(before, "Estado actual")

        if args.dry_run:
            print("  (--dry-run: nada foi escrito)")
            return 0, 0

        started = time.monotonic()
        cur = conn.cursor()
        stmts = rows = pending = 0
        year = None
        year_rows = 0
        year_days = set()
        duplicates = DuplicateReport()

        def flush_year():
            if year is not None:
                print("      %s: %s linhas em %s dias" % (year, fmt_int(year_rows), len(year_days)))

        for stmt in iter_plan(args.file, args, duplicates):
            if stmt.day and stmt.day[:4] != year:
                flush_year()
                year = stmt.day[:4]
                year_rows = 0
                year_days = set()

            if stmt.kind == "refresh":
                flush_year()
                year = None
                print("  A refrescar as materialized views de metric (pode demorar)...")

            try:
                cur.execute(stmt.sql)
            except psycopg2.Error as exc:
                conn.rollback()
                detail = str(exc).strip()
                if "affect row a second time" in detail:
                    detail += ("\n    (linhas repetidas no mesmo INSERT; use --on-duplicate sum"
                               " em vez de --on-duplicate error)")
                raise ImportFailed("linha %s do ficheiro: %s\n    %s" % (stmt.line, detail, stmt.head()))

            stmts += 1
            rows += stmt.rows
            if stmt.day:
                year_rows += stmt.rows
                year_days.add(stmt.day)
            pending += 1
            if args.commit_every and pending >= args.commit_every:
                conn.commit()
                pending = 0

        flush_year()
        conn.commit()
        cur.close()
        elapsed = time.monotonic() - started
        print("  %s instrucoes aplicadas (%s linhas) em %.1f s"
              % (fmt_int(stmts), fmt_int(rows), elapsed))
        if duplicates.rows:
            print("  %s" % duplicates.describe(args.on_duplicate)[0])

        print_snapshot_diff(before, snapshot(conn))
        return stmts, rows
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Destino MongoDB (udata.metrics)
# --------------------------------------------------------------------------- #
#
# A serie diaria vai para a coleccao `metrics` do udata, na forma que la esta
# desde a importacao Matomo de 2021-2023:
#
#     {_id: ObjectId(...), object_id: ObjectId|str, date: "YYYY-MM-DD",
#      level: "daily", values: {..., nb_visits: N}}
#
# O upsert e por (object_id, date, level) e so escreve `values.nb_visits`: as
# outras chaves de `values` (datasets, members, nb_hits, ...) pertencem ao udata
# e ao DAG metrics_etl e ficam como estao. O DAG so escreve o documento do site
# (`object_id: "dados.gov.pt"`), por isso nao ha dois autores no mesmo campo.

MONGO_VALUE_KEY = "nb_visits"

# Tabela do dump -> (tipo de objecto, coluna do id, coleccao do udata que o valida)
MONGO_OBJECTS = {
    "visits_datasets": ("dataset", "dataset_id", "dataset"),
    "visits_reuses": ("reuse", "reuse_id", "reuse"),
    "visits_organizations": ("organization", "organization_id", "organization"),
    "visits_dataservices": ("dataservice", "dataservice_id", "dataservice"),
    "visits_resources": ("resource", "resource_id", None),  # UUID embebido no dataset
}

_OBJECTID_RE = re.compile(r"^[a-f0-9]{24}$", re.I)


def mongo_settings(args):
    """
    (uri, base de dados, coleccao) do destino MongoDB, ou None se nao configurado.

    Nao ha host por omissao: o MongoDB esta noutra VM e muda de ambiente para
    ambiente, portanto e melhor parar com uma mensagem do que escrever no sitio
    errado por causa de um valor deixado no codigo.
    """
    uri = args.mongo_uri or os.environ.get("METRICS_MONGO_URI")
    if not uri:
        host = os.environ.get("MONGODB_HOST")
        if not host:
            return None
        uri = "mongodb://%s:%s" % (host, os.environ.get("MONGODB_PORT", "27017"))
    return uri, args.mongo_db, args.mongo_collection


MONGO_NAO_CONFIGURADO = """ERRO: falta a configuracao do MongoDB (esta noutra VM, nao tem valor por omissao).

Indique uma destas:
  --mongo-uri "mongodb://utilizador:password@host:27017"
  METRICS_MONGO_URI=...            no ambiente ou no .env
  MONGODB_HOST=...  (+ MONGODB_PORT, por omissao 27017)

O .env lido por omissao e o que estiver ao lado do script (%s);
--env-file aponta para outro. Para importar so para o PostgreSQL: --skip-mongo.
""" % DEFAULT_ENV_FILE


def redact_uri(uri):
    """Esconde a password de um URI mongodb://user:pass@host."""
    return re.sub(r"://[^/@]*:[^/@]*@", "://***@", uri)


def to_object_id(raw):
    """ObjectId quando o id tem a forma de um (datasets, reuses, ...); string nos UUID dos recursos."""
    from bson import ObjectId
    return ObjectId(raw) if _OBJECTID_RE.match(raw or "") else raw


def iter_metric_rows(path, args, report=None):
    """Gera (tipo, object_id, dia, nb_visit) a partir dos INSERTs do dump."""
    for stmt in iter_plan(path, args, report):
        if stmt.kind != "insert":
            continue
        target = MONGO_OBJECTS.get(stmt.short_table)
        if target is None:
            continue
        obj_type, id_column, _collection = target
        parsed = parse_insert(stmt.sql)
        if parsed is None:
            sys.stderr.write("AVISO: INSERT na linha %s nao foi possivel interpretar para o MongoDB\n"
                             % stmt.line)
            continue
        _table, columns, rows, _tail = parsed
        try:
            id_idx = columns.index(id_column)
            date_idx = columns.index("date_metric")
            visit_idx = columns.index("nb_visit")
        except ValueError:
            continue
        for fields in rows:
            yield (obj_type,
                   unquote(fields[id_idx]),
                   unquote(fields[date_idx]),
                   unquote(fields[visit_idx]) or 0)


def build_known_ids(db):
    """Ids que existem mesmo no udata, para assinalar metricas de objectos apagados."""
    known = {}
    for obj_type, collection in (("dataset", "dataset"), ("reuse", "reuse"),
                                 ("organization", "organization"), ("dataservice", "dataservice")):
        if collection in db.list_collection_names():
            known[obj_type] = {str(doc["_id"]) for doc in db[collection].find({}, {"_id": 1})}
    return known


def ensure_mongo_index(coll, dry_run=False):
    """
    Garante um indice que sirva o filtro do upsert (object_id + date).

    A `udata.metrics` ja tem `object_id_1_date_1`, portanto aqui nao se cria nada;
    numa coleccao nova sem indice cada upsert faria varrimento completo e a
    importacao arrastava-se durante horas.
    """
    for spec in coll.index_information().values():
        keys = [key for key, _direction in spec.get("key", [])]
        if keys[:1] == ["object_id"] and "date" in keys:
            return None
    if dry_run:
        return "em falta (--dry-run: nao foi criado)"
    coll.create_index([("object_id", 1), ("date", 1), ("level", 1)], background=True)
    return "criado object_id_1_date_1_level_1"


def mongo_snapshot(coll):
    """Contagens da coleccao antes/depois, para se ver o efeito da importacao."""
    total = coll.estimated_document_count()
    query = {"values.%s" % MONGO_VALUE_KEY: {"$exists": True}}
    with_visits = coll.count_documents(query)
    first = last = None
    if with_visits:
        first = coll.find(query, {"date": 1}, sort=[("date", 1)]).limit(1)[0].get("date")
        last = coll.find(query, {"date": 1}, sort=[("date", -1)]).limit(1)[0].get("date")
    return {"total": total, "with_visits": with_visits, "first": first, "last": last}


def print_mongo_snapshot(snap, title):
    print("  %s:" % title)
    print("      documentos            %10s" % fmt_int(snap["total"]))
    print("      com values.%-10s %10s%s"
          % (MONGO_VALUE_KEY, fmt_int(snap["with_visits"]),
             ("   %s .. %s" % (snap["first"], snap["last"])) if snap["with_visits"] else ""))


def import_into_mongo(args, settings):
    """Escreve a serie diaria do dump na coleccao metrics do udata."""
    try:
        from pymongo import MongoClient, UpdateOne
        from pymongo.errors import BulkWriteError, PyMongoError
    except ImportError:
        raise ImportFailed("falta o pymongo (pip install pymongo)")

    uri, dbname, collname = settings
    print("\n=== %s/%s.%s ===" % (redact_uri(uri), dbname, collname))

    client = MongoClient(uri, serverSelectionTimeoutMS=int(args.mongo_timeout * 1000))
    try:
        db = client[dbname]
        coll = db[collname]
        before = mongo_snapshot(coll)
        print_mongo_snapshot(before, "Estado actual")

        index_note = ensure_mongo_index(coll, args.dry_run)
        if index_note:
            print("      indice para o upsert: %s" % index_note)

        known = build_known_ids(db)
        unknown = {}
        by_type = {}
        stats = {"rows": 0, "written": 0, "inserted": 0, "modified": 0, "unchanged": 0}
        duplicates = DuplicateReport()
        year = None
        year_rows = 0
        batch = []

        def flush():
            if not batch:
                return
            if args.dry_run:
                batch[:] = []
                return
            try:
                result = coll.bulk_write(batch, ordered=False)
            except BulkWriteError as exc:
                errors = exc.details.get("writeErrors", [])
                raise ImportFailed("%d escritas falharam; primeira: %s"
                                   % (len(errors), errors[0].get("errmsg") if errors else "?"))
            stats["inserted"] += result.upserted_count
            stats["modified"] += result.modified_count
            # Os que ja la estavam com o mesmo valor. Contados para os numeros
            # fecharem: escritos = novos + alterados + iguais.
            stats["unchanged"] += result.matched_count - result.modified_count
            stats["written"] += len(batch)
            batch[:] = []

        started = time.monotonic()
        for obj_type, obj_id, day, nb_visit in iter_metric_rows(args.file, args, duplicates):
            stats["rows"] += 1
            if day and day[:4] != year:
                if year is not None:
                    print("      %s: %s linhas" % (year, fmt_int(year_rows)))
                year = day[:4]
                year_rows = 0
            year_rows += 1

            if obj_type in known and obj_id not in known[obj_type]:
                unknown[obj_type] = unknown.get(obj_type, 0) + 1
                if args.mongo_skip_unknown:
                    continue

            by_type[obj_type] = by_type.get(obj_type, 0) + 1
            batch.append(UpdateOne(
                {"object_id": to_object_id(obj_id), "date": day, "level": "daily"},
                {"$set": {"values.%s" % MONGO_VALUE_KEY: nb_visit}},
                upsert=True,
            ))
            if len(batch) >= args.mongo_batch:
                flush()
        flush()
        if year is not None:
            print("      %s: %s linhas" % (year, fmt_int(year_rows)))

        if args.dry_run:
            print("  %s linhas seriam escritas em %s documentos (--dry-run: nada foi escrito)"
                  % (fmt_int(stats["rows"]), fmt_int(sum(by_type.values()))))
        else:
            print("  %s linhas -> %s documentos (%s novos, %s alterados, %s iguais) em %.1f s"
                  % (fmt_int(stats["rows"]), fmt_int(stats["written"]), fmt_int(stats["inserted"]),
                     fmt_int(stats["modified"]), fmt_int(stats["unchanged"]),
                     time.monotonic() - started))
        if by_type:
            print("      por tipo: %s" % " | ".join(
                "%s %s" % (name, fmt_int(count)) for name, count in sorted(by_type.items())))
        if unknown:
            print("      ids que ja nao existem no udata: %s%s" % (
                " | ".join("%s %s" % (name, fmt_int(count)) for name, count in sorted(unknown.items())),
                " (ignorados)" if args.mongo_skip_unknown else " (escritos na mesma; --mongo-skip-unknown ignora-os)"))

        if not args.dry_run:
            print_mongo_snapshot(mongo_snapshot(coll), "Resultado")
        return stats["rows"]
    except PyMongoError as exc:
        raise ImportFailed(str(exc).strip())
    finally:
        client.close()


# --------------------------------------------------------------------------- #

def normalise_tables(raw):
    """'datasets, resources' -> {'visits_datasets', 'visits_resources'}."""
    wanted = set()
    for token in raw.split(","):
        name = token.strip().lower().rsplit(".", 1)[-1]
        if not name:
            continue
        if not name.startswith(("visits_", "matomo_")):
            name = "visits_" + name
        wanted.add(name)
    unknown = wanted - set(METRIC_TABLES)
    if unknown:
        sys.stderr.write("AVISO: tabelas desconhecidas em --tables: %s\n" % ", ".join(sorted(unknown)))
    return wanted


def valid_date(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError("data invalida (esperado YYYY-MM-DD): %s" % value)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Importa o dump metrics-full.sql para as bases de dados de metricas (PostgreSQL, schema metric).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Sem opcoes, importa o ficheiro todo para %s e para o MongoDB configurado."
               % dsn_label(pg_ext.make_dsn(**default_pg())),
    )
    parser.add_argument("-f", "--file", default=DEFAULT_SQL_FILE,
                        help="dump a importar (def. metrics-full.sql ao lado do script; aceita .gz/.bz2/.xz)")
    parser.add_argument("--target", action="append", metavar="DSN",
                        help="destino PostgreSQL (libpq ou URL); repetivel para importar para varias bases de dados")
    parser.add_argument("--date-from", type=valid_date, help="so os dias a partir desta data (YYYY-MM-DD)")
    parser.add_argument("--date-to", type=valid_date, help="so os dias ate esta data (YYYY-MM-DD)")
    parser.add_argument("--tables", help="so estas tabelas (ex.: datasets,resources ou visits_datasets)")
    parser.add_argument("--skip-ddl", action="store_true",
                        help="nao aplica CREATE SCHEMA/TABLE/INDEX; usa o que ja existe no destino")
    parser.add_argument("--no-refresh", action="store_true",
                        help="nao refresca as materialized views no fim")
    parser.add_argument("--on-duplicate", choices=("sum", "first", "last", "error"), default="sum",
                        help="linhas repetidas (mesmo objecto e dia) dentro do mesmo INSERT: "
                             "sum soma as visitas (def.), first/last ficam com uma delas, "
                             "error deixa o PostgreSQL rejeitar como faz o psql")
    parser.add_argument("--commit-every", type=int, default=0, metavar="N",
                        help="faz COMMIT a cada N instrucoes (def. 0 = tudo numa transaccao)")
    parser.add_argument("--mongo-uri", metavar="URI",
                        help="MongoDB de destino (def. env METRICS_MONGO_URI, ou MONGODB_HOST/PORT)")
    parser.add_argument("--mongo-db", default=os.environ.get("METRICS_MONGO_DB", "udata"),
                        help="base de dados do MongoDB (def. udata)")
    parser.add_argument("--mongo-collection",
                        default=os.environ.get("METRICS_MONGO_COLLECTION", "metrics"),
                        help="coleccao da serie diaria (def. metrics)")
    parser.add_argument("--mongo-batch", type=int, default=2000, metavar="N",
                        help="tamanho do lote de escritas no MongoDB (def. 2000)")
    parser.add_argument("--mongo-timeout", type=float, default=10.0, metavar="SEG",
                        help="tempo maximo a espera do MongoDB (def. 10)")
    parser.add_argument("--mongo-skip-unknown", action="store_true",
                        help="nao escreve metricas de objectos que ja nao existem no udata")
    parser.add_argument("--skip-mongo", action="store_true", help="nao escreve no MongoDB")
    parser.add_argument("--skip-postgres", action="store_true", help="nao escreve no PostgreSQL")
    parser.add_argument("--dry-run", action="store_true",
                        help="mostra o plano e o estado dos destinos, sem escrever nada")
    parser.add_argument("--info", action="store_true",
                        help="so analisa o ficheiro; nao liga a nenhuma base de dados")
    parser.add_argument("--env-file", metavar="PATH",
                        help="ficheiro .env a carregar (def. o que estiver ao lado do script); "
                             "o que ja estiver exportado no ambiente ganha ao ficheiro")
    parser.add_argument("--version", action="version", version="%(prog)s " + SCRIPT_VERSION)
    args = parser.parse_args(argv)

    if args.date_from and args.date_to and args.date_from > args.date_to:
        parser.error("--date-from e posterior a --date-to")
    if args.commit_every < 0:
        parser.error("--commit-every tem de ser >= 0")
    args.tables = normalise_tables(args.tables) if args.tables else None
    return args


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    # O .env tem de ser lido antes do argparse: e de la que saem alguns valores
    # por omissao (--mongo-db, e o destino PostgreSQL do epilogo da ajuda).
    env_path, env_vars = bootstrap_env(argv)
    args = parse_args(argv)

    if not os.path.exists(args.file):
        sys.stderr.write("ERRO: ficheiro nao encontrado: %s\n" % args.file)
        return 2

    # Validar a configuracao antes de analisar o ficheiro (sao 30 MB) e, sobretudo,
    # antes de escrever no PostgreSQL: nada de importacoes a meio por falta de config.
    mongo = None if (args.skip_mongo or args.info) else mongo_settings(args)
    if not (args.skip_mongo or args.info) and mongo is None:
        sys.stderr.write(MONGO_NAO_CONFIGURADO)
        return 2

    if env_vars:
        print("Configuracao: %s (%d variavel(is))" % (os.path.abspath(env_path), env_vars))
    print("Ficheiro: %s (%s)" % (os.path.abspath(args.file), fmt_size(os.path.getsize(args.file))))
    for line in read_dump_header(args.file):
        print("  %s" % line)

    plan = summarise(args.file, args)
    print_plan(plan, args)

    if args.info:
        return 0

    if not (plan["inserts"] or plan["ddl"] or plan["refresh"] or plan["other"]):
        print("\nNada a aplicar com os filtros indicados.")
        return 0

    targets = [] if args.skip_postgres else resolve_targets(args)
    destinations = [dsn_label(dsn) for dsn in targets]
    if mongo:
        destinations.append("%s/%s.%s" % (redact_uri(mongo[0]), mongo[1], mongo[2]))
    if not destinations:
        print("\nNada a fazer: --skip-postgres e --skip-mongo ao mesmo tempo.")
        return 0
    print("\nDestinos: %s" % ", ".join(destinations))

    failures = []
    for dsn in targets:
        try:
            import_into(dsn, args)
        except (ImportFailed, psycopg2.Error) as exc:
            failures.append((dsn_label(dsn), str(exc).strip()))
            sys.stderr.write("ERRO em %s: %s\n" % (dsn_label(dsn), str(exc).strip()))

    if mongo:
        label = "%s/%s.%s" % (redact_uri(mongo[0]), mongo[1], mongo[2])
        try:
            import_into_mongo(args, mongo)
        except ImportFailed as exc:
            failures.append((label, str(exc).strip()))
            sys.stderr.write("ERRO em %s: %s\n" % (label, str(exc).strip()))

    if failures:
        print("\nTerminou com erros em %d de %d destino(s):" % (len(failures), len(destinations)))
        for label, message in failures:
            print("  %s: %s" % (label, message.splitlines()[0]))
        return 1

    print("\nConcluido%s." % (" (simulacao)" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
