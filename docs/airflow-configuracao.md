# Configuracao do Apache Airflow — Data Engineering Stack

Este documento descreve passo a passo como configurar o Airflow para funcionar com os servicos do ecossistema: **udata**, **api-tabular (PostgREST)**, **Hydra (PostgreSQL)** e **MongoDB**.

---

## Indice

1. [Pre-requisitos](#1-pre-requisitos)
2. [Arranque dos containers](#2-arranque-dos-containers)
3. [Configuracao de rede](#3-configuracao-de-rede)
4. [Conexoes (Connections)](#4-conexoes-connections)
5. [Indices e constraints no PostgreSQL](#5-indices-e-constraints-no-postgresql)
6. [Variaveis (Variables)](#6-variaveis-variables)
7. [DAG metrics_etl — Pipeline de metricas](#7-dag-metrics_etl--pipeline-de-metricas)
8. [Arquitetura do fluxo de metricas](#8-arquitetura-do-fluxo-de-metricas)
9. [Resolucao de problemas](#9-resolucao-de-problemas)

---

## 1. Pre-requisitos

- Docker e Docker Compose instalados
- Servicos a correr:

| Servico | Porta | Binding | Notas |
|---------|-------|---------|-------|
| **udata** (backend API) | `7000` | `127.0.0.1` (host) | Corre no host, nao em Docker |
| **udata-front** (Next.js) | `3000` | host | Frontend dados.gov |
| **MongoDB** (udata) | `27017` | `0.0.0.0` | Container `udata-front-pt-db-1` |
| **Redis** (broker) | `6379` | `0.0.0.0` | Container `udata-front-pt-broker-1` |
| **Hydra PostgreSQL** | `5432` | `0.0.0.0` | Container `hydra-pt-database-1` |
| **Hydra PostgreSQL CSV** | `5434` | `0.0.0.0` | Container `hydra-pt-database-csv-1` |
| **PostgREST** | `8080` | `0.0.0.0` | Container `api-tabular-pt-postgrest-1` |
| **API Tabular** (tabular) | `8005` | host | Leitura de recursos CSV |
| **API Metrics** | `8006` | host | Leitura de metricas agregadas |

- Ficheiro `.env` configurado na raiz do projeto `data-engineering-stack/`

**Importante:** O udata esta bound a `127.0.0.1:7000`, pelo que o container Airflow **nao consegue aceder** a ele via HTTP. O DAG acede ao MongoDB directamente (porta `27017` em `0.0.0.0`).

## 2. Arranque dos containers

```bash
cd data-engineering-stack

# Construir a imagem (primeira vez ou apos alteracoes ao Dockerfile/requirements.txt)
docker compose build --no-cache webserver

# Arrancar os containers
docker compose up -d

# Verificar estado
docker compose ps
```

O Airflow estara disponivel em `http://localhost:18080`.

**Credenciais de acesso** (definidas no `.env`):
- Username: valor de `AIRFLOW_ADMIN_NAME`
- Password: valor de `AIRFLOW_ADMIN_PASSWORD`

O entrypoint (`scripts/airflow-entrypoint.sh`) executa automaticamente:
1. `airflow db migrate` — inicializa/atualiza a base de dados
2. Cria o utilizador admin
3. Inicia o scheduler em background
4. Inicia o webserver

## 3. Configuracao de rede

### host.docker.internal

No `docker-compose.yml`, o servico `webserver` ja tem:

```yaml
webserver:
  extra_hosts:
    - "host.docker.internal:host-gateway"
```

Isto mapeia `host.docker.internal` para o IP do host. **Mas** se o servico no host so escuta em `127.0.0.1` (como o udata na porta 7000), o container nao consegue aceder.

### Servicos acessiveis vs nao acessiveis a partir do container

| Servico | IP no DAG | Acessivel? | Razao |
|---------|-----------|------------|-------|
| MongoDB (`27017`) | `10.55.37.143` | Sim | Bound a `0.0.0.0` |
| PostgreSQL CSV (`5434`) | `10.55.37.145` (via Airflow Connection) | Sim | Bound a `0.0.0.0` |
| API Metrics (`8006`) | `10.55.37.145` | Sim | Bound a `0.0.0.0` |
| PostgREST (`8080`) | `10.55.37.145` | Sim | Bound a `0.0.0.0` |
| udata API (`7000`) | N/A | **Nao** | Bound a `127.0.0.1` |

Para verificar o acesso a partir do container:

```bash
docker exec <container> getent hosts host.docker.internal
docker exec <container> python3 -c "from pymongo import MongoClient; print(MongoClient('10.55.37.143', 27017).list_database_names())"
```

## 4. Conexoes (Connections)

Aceder a **Admin > Connections** no UI do Airflow (`http://localhost:18080/connection/list/`), ou usar o CLI.

**Substituir `<container>` pelo nome real**, ex: `airflow-demo-test`.

### 4.1. Hydra PostgreSQL — Base de dados principal

```bash
docker exec <container> airflow connections add "hydra_postgres" \
  --conn-type postgres \
  --conn-host "10.55.37.145" \
  --conn-port 5432 \
  --conn-login postgres \
  --conn-password postgres \
  --conn-schema postgres \
  --conn-description "Hydra main PostgreSQL database"
```

### 4.2. Hydra PostgreSQL CSV — Base de dados usada pelo api-tabular/PostgREST

```bash
docker exec <container> airflow connections add "hydra_postgres_csv" \
  --conn-type postgres \
  --conn-host "10.55.37.145" \
  --conn-port 5434 \
  --conn-login postgres \
  --conn-password postgres \
  --conn-schema postgres \
  --conn-description "Hydra CSV PostgreSQL database (used by api-tabular/PostgREST)"
```

### 4.3. API Tabular (HTTP)

```bash
docker exec <container> airflow connections add "api_tabular_conn" \
  --conn-type http \
  --conn-host "10.55.37.145" \
  --conn-port 8006 \
  --conn-description "API Tabular (PostgREST)"
```

### 4.4. MongoDB (udata)

```bash
docker exec <container> airflow connections add "mongo_default" \
  --conn-type mongo \
  --conn-host "10.55.37.145" \
  --conn-port 27017 \
  --conn-description "MongoDB udata (sem autenticacao)"
```

### 4.5. udata API (HTTP)

```bash
docker exec <container> airflow connections add "udata_http" \
  --conn-type http \
  --conn-host "172.31.204.12" \
  --conn-port 7000 \
  --conn-description "udata API"
```

### Resumo de conexoes

| Conn Id | Type | Host | Port | Schema | Usado por |
|---------|------|------|------|--------|-----------|
| `hydra_postgres` | postgres | `10.55.37.145` | `5432` | `postgres` | Acesso a BD principal |
| `hydra_postgres_csv` | postgres | `10.55.37.145` | `5434` | `postgres` | DAG `metrics_etl` (escrita metricas no schema `metric`) |
| `api_tabular_conn` | http | `10.55.37.145` | `8006` | — | Referencia a API Metrics |
| `mongo_default` | mongo | `10.55.37.145` | `27017` | — | DAG `metrics_etl` (logs) |
| `udata_http` | http | `172.31.204.12` | `7000` | — | Referencia (nao usado no DAG actual) |

### Verificar todas as conexoes

```bash
docker exec <container> airflow connections list -o table
```

## 5. Indices e constraints no PostgreSQL

O DAG `metrics_etl` escreve nas tabelas base do schema `metric` e usa UPSERT. Os indices unicos sao criados automaticamente pelo DAG, mas podem ser criados manualmente:

```bash
# Indice unico para upsert na tabela visits_datasets
docker exec hydra-pt-database-csv-1 psql -U postgres -c \
  "CREATE UNIQUE INDEX IF NOT EXISTS visits_datasets_upsert_idx ON metric.visits_datasets (dataset_id, date_metric);"

# Indice unico para upsert na tabela visits_resources
docker exec hydra-pt-database-csv-1 psql -U postgres -c \
  "CREATE UNIQUE INDEX IF NOT EXISTS visits_resources_upsert_idx ON metric.visits_resources (resource_id, date_metric);"
```

### Tabelas base no schema `metric`

O DAG escreve nestas tabelas base:

| Tabela | Descricao |
|--------|-----------|
| `metric.visits_datasets` | Visitas diarias por dataset (colunas: `date_metric`, `dataset_id`, `organization_id`, `nb_visit`) |
| `metric.visits_resources` | Downloads diarios por recurso (colunas: `date_metric`, `resource_id`, `dataset_id`, `organization_id`, `nb_visit`) |

### Materialized views

As tabelas base sao agregadas por materialized views (refrescadas automaticamente pelo DAG):

| View | Fonte | Descricao |
|------|-------|-----------|
| `metric.metrics_datasets` | `visits_datasets` + `matomo_datasets` + `visits_resources` | Juncao de metricas diarias por dataset |
| `metric.datasets` | `metrics_datasets` | Agregacao mensal por dataset |
| `metric.datasets_total` | `metrics_datasets` | Total acumulado por dataset (usada pelo PostgREST/API) |
| `metric.resources` | `visits_resources` | Agregacao mensal por recurso |
| `metric.resources_total` | `visits_resources` | Total acumulado por recurso |
| `metric.site` | `datasets` + `reuses` + `dataservices` | Metricas mensais do site |

### Verificar que as tabelas existem

```bash
docker exec hydra-pt-database-csv-1 psql -U postgres -c "\dt metric.*; \dm metric.*;"
```

## 6. Variaveis (Variables)

```bash
docker exec <container> airflow variables set UDATA_INSTANCE_URL "http://172.31.204.12:7000"
docker exec <container> airflow variables set METRICS_API_URL "http://10.55.37.145:8006/api"
docker exec <container> airflow variables set MONGODB_CONN_ID "mongo_default"
```

**Nota:** O DAG `metrics_etl` actual usa constantes no codigo em vez de Variables, para simplicidade.

## 7. DAG metrics_etl — Pipeline de metricas

### Ficheiro

`dags/metrics_etl.py`

### Fluxo

```
extract_tracking_events → send_to_metrics_db → refresh_materialized_views → update_udata_metrics → save_to_mongodb
```

### Tasks

| Task | O que faz | Origem | Destino |
|------|-----------|--------|---------|
| `extract_tracking_events` | Agrega views e downloads da collection `tracking_events` por dia, constroi lookups de org/resource, calcula contagens do site | MongoDB `udata.tracking_events` + contagens de collections | XCom |
| `send_to_metrics_db` | Escreve visitas diarias por dataset e downloads por recurso nas tabelas base do schema `metric` | XCom | PostgreSQL `metric.visits_datasets` + `metric.visits_resources` (porta 5434) |
| `refresh_materialized_views` | Refresca todas as materialized views para que o PostgREST sirva dados actualizados | PostgreSQL | PostgreSQL (15 materialized views) |
| `update_udata_metrics` | Le totais do PostgREST (`datasets_total`) e escreve no MongoDB do udata; actualiza metricas de datasets, resources, organizations, reuses, dataservices e site | API Metrics `datasets_total` + MongoDB aggregations | MongoDB `udata.dataset[].metrics`, `udata.organization[].metrics`, `udata.reuse[].metrics`, `udata.metrics`, `udata.site` |
| `save_to_mongodb` | Regista log do processo ETL | XCom | MongoDB `etl_logs.metrics_logs` |

### Schedule

O DAG esta configurado com `schedule="* * * * *"` (cada minuto) para testes.

**Para producao**, alterar para uma frequencia adequada:

```python
schedule="0 */2 * * *"  # cada 2 horas
schedule="@hourly"       # cada hora
schedule="@daily"        # diario
```

### Constantes no DAG

```python
UDATA_MONGO_HOST = "10.55.37.143"    # IP do servidor MongoDB
UDATA_MONGO_PORT = 27017
UDATA_MONGO_DB = "udata"             # Base de dados MongoDB do udata
METRICS_PG_CONN_ID = "hydra_postgres_csv"  # Airflow Connection ID
METRICS_MONGO_DB = "etl_logs"    # BD para logs do ETL
METRICS_API_URL = "http://10.55.37.145:8006/api"  # API Metrics (read)
```

### MongoHook — API correcta (v4.2.2)

O provider `apache-airflow-providers-mongo==4.2.2` usa `mongo_collection` como primeiro argumento:

```python
# CORRECTO
hook.insert_one(mongo_collection="metrics_logs", doc=log_doc, mongo_db="etl_logs")

# ERRADO (versoes anteriores)
hook.insert_one(collection="metrics_logs", doc=log_doc, mongo_db="etl_logs")
```

## 8. Arquitetura do fluxo de metricas

```
                      DAG metrics_etl (Airflow)
                      ===========================

  [1] EXTRACT                    [2] SEND TO PG
  MongoDB udata                  PostgreSQL CSV (5434)
  (porta 27017)                  schema: metric
  +---------------------+       +---------------------------+
  | tracking_events     |       | visits_datasets           |
  |  .event_type: view  |--agg-->|  date_metric, dataset_id |
  |  .event_type: download|     |  nb_visit                 |
  |  .object_id         |       +---------------------------+
  |  .resource_id       |       | visits_resources          |
  |  .created_at        |--agg-->|  date_metric, resource_id|
  +---------------------+       |  dataset_id, nb_visit     |
                                +---------------------------+
                                        |
  [3] REFRESH VIEWS              [materialized views]
  PostgreSQL CSV (5434)          metrics_datasets
                                 → datasets / datasets_total
                                 → resources / resources_total
                                 → site
                                        |
  [4] UPDATE UDATA              [PostgREST API]
  MongoDB udata                         |
  (porta 27017)                  +-------------------+
  +---------------------+       | API Metrics:8006  |
  | dataset[].metrics   |<------| /api/datasets_    |
  |   .views            |       |   total/data/     |
  |   .resources_downloads|     +-------------------+
  |   .followers         |
  |   .discussions       |
  | organization[].metrics|
  |   .views, .datasets  |
  | reuse[].metrics      |
  | site[].metrics       |
  | metrics (daily)      |
  +---------------------+

  [5] LOG
  MongoDB etl_logs
  +---------------------+
  | metrics_logs        |
  | (registo de cada    |
  |  execucao do DAG)   |
  +---------------------+
```

### Como o udata le as metricas

O udata tem um job celery `update-metrics` (plugin `udata_metrics`) que:
1. Le de `{METRICS_API}/datasets_total/data/?visit__greater=1`
2. Atualiza `udata.dataset[].metrics.views` e `.resources_downloads`

**O celery nao esta a correr**, pelo que o DAG faz este trabalho na task `update_udata_metrics`.

Configuracao relevante no udata (`udata.cfg` / `.env`):

```
METRICS_API=http://10.55.37.145:8006/api
```

### Tabelas no PostgreSQL CSV (porta 5434, schema `metric`)

**Tabelas base (escritas pelo DAG):**

| Tabela | Descricao |
|--------|-----------|
| `metric.visits_datasets` | Visitas diarias por dataset (`date_metric`, `dataset_id`, `nb_visit`) |
| `metric.visits_resources` | Downloads diarios por recurso (`date_metric`, `resource_id`, `dataset_id`, `nb_visit`) |
| `metric.matomo_datasets` | Outlinks do Matomo por dataset (nao escrita pelo DAG) |

**Materialized views (refrescadas pelo DAG):**

| View | Descricao |
|------|-----------|
| `metric.metrics_datasets` | Juncao de visits + matomo + resource downloads por dataset/dia |
| `metric.datasets` | Agregacao mensal por dataset |
| `metric.datasets_total` | Total acumulado por dataset (exposta pelo PostgREST) |
| `metric.resources` | Agregacao mensal por recurso |
| `metric.resources_total` | Total acumulado por recurso |
| `metric.site` | Metricas mensais do site |
| `metric.organizations` / `metric.reuses` / `metric.dataservices` | Agregacoes por tipo de objecto |

### Collections no MongoDB

| Database | Collection | Descricao |
|----------|-----------|-----------|
| `udata` | `dataset` | Datasets com metricas embebidas em `metrics.*` |
| `udata` | `metrics` | Metricas diarias do site (formato: `{date, level, object_id, values}`) |
| `udata` | `tracking_events` | Eventos de tracking (view, download) — fonte principal de metricas |
| `udata` | `metric_event` | Eventos de tracking legados (api_call, download) |
| `etl_logs` | `metrics_logs` | Logs de execucao do DAG |

## 9. Resolucao de problemas

### Container nao acede ao udata (porta 7000)

O udata esta bound a `127.0.0.1:7000` — so aceita ligacoes locais. Verificar:

```bash
ss -tlnp | grep 7000
# LISTEN  127.0.0.1:7000  → NAO acessivel do container
# LISTEN  0.0.0.0:7000    → acessivel do container
```

O DAG contorna este problema acedendo ao MongoDB directamente (porta 27017, bound a `0.0.0.0`).

### ObjectId nao serializavel para XCom

Ao extrair dados do MongoDB, excluir o campo `_id` na projecao:

```python
pipeline = [
    {"$project": {"_id": 0, "dataset_id": {"$toString": "$_id"}, ...}},
]
```

### MongoHook.insert_one() — TypeError missing argument

Usar `mongo_collection` em vez de `collection`:

```python
# v4.2.2+
hook.insert_one(mongo_collection="col_name", doc=doc, mongo_db="db_name")
```

### ON CONFLICT sem indice unico

O UPSERT (`ON CONFLICT ... DO UPDATE`) requer um indice unico. Ver [seccao 5](#5-indices-e-constraints-no-postgresql).

### DAG com "Import Errors"

```bash
docker exec <container> airflow dags list-import-errors
```

Causas comuns:
- Modulos nao instalados (adicionar ao `requirements.txt` e reconstruir)
- Erros de sintaxe no ficheiro Python
- `_id: 0` em falta nas projecoes MongoDB (ObjectId nao serializavel)

### Verificar conectividade a partir do container

```bash
# MongoDB
docker exec <container> python3 -c "
from pymongo import MongoClient
c = MongoClient('10.55.37.143', 27017)
print(c.list_database_names())
"

# PostgreSQL (schema metric)
docker exec <container> python3 -c "
from airflow.providers.postgres.hooks.postgres import PostgresHook
hook = PostgresHook(postgres_conn_id='hydra_postgres_csv')
conn = hook.get_conn()
cur = conn.cursor()
cur.execute('SELECT count(*) FROM metric.visits_datasets')
print('visits_datasets:', cur.fetchone()[0])
"

# API Metrics
docker exec <container> python3 -c "
import requests
r = requests.get('http://10.55.37.145:8006/api/datasets_total/data/?page_size=1')
print(r.json())
"
```

### Logs do DAG

```bash
# Logs do ultimo run
docker compose logs --tail=50 webserver | grep metrics_etl

# Log de uma task especifica
docker exec <container> airflow tasks test metrics_etl extract_tracking_events 2026-03-18
```

### Reconstruir a imagem apos alteracoes

```bash
docker compose build --no-cache webserver
docker compose up -d
```
