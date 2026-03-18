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
7. [DAG exemplo_etl — Pipeline de metricas](#7-dag-exemplo_etl--pipeline-de-metricas)
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
| MongoDB (`27017`) | `192.168.1.96` | Sim | Bound a `0.0.0.0` |
| PostgreSQL CSV (`5434`) | via Airflow Connection | Sim | Bound a `0.0.0.0` |
| API Metrics (`8006`) | `192.168.1.96` | Sim | Bound a `0.0.0.0` |
| PostgREST (`8080`) | `192.168.1.96` | Sim | Bound a `0.0.0.0` |
| udata API (`7000`) | N/A | **Nao** | Bound a `127.0.0.1` |

Para verificar o acesso a partir do container:

```bash
docker exec <container> getent hosts host.docker.internal
docker exec <container> python3 -c "from pymongo import MongoClient; print(MongoClient('192.168.1.96', 27017).list_database_names())"
```

## 4. Conexoes (Connections)

Aceder a **Admin > Connections** no UI do Airflow (`http://localhost:18080/connection/list/`), ou usar o CLI.

**Substituir `<container>` pelo nome real**, ex: `airflow-demo-test`.

### 4.1. Hydra PostgreSQL — Base de dados principal

```bash
docker exec <container> airflow connections add "hydra_postgres" \
  --conn-type postgres \
  --conn-host "192.168.1.96" \
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
  --conn-host "192.168.1.96" \
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
  --conn-host "192.168.1.96" \
  --conn-port 8006 \
  --conn-description "API Tabular (PostgREST)"
```

### 4.4. MongoDB (udata)

```bash
docker exec <container> airflow connections add "mongo_default" \
  --conn-type mongo \
  --conn-host "192.168.1.96" \
  --conn-port 27017 \
  --conn-description "MongoDB udata (sem autenticacao)"
```

### 4.5. udata API (HTTP)

```bash
docker exec <container> airflow connections add "udata_http" \
  --conn-type http \
  --conn-host "192.168.1.96" \
  --conn-port 7000 \
  --conn-description "udata API"
```

### Resumo de conexoes

| Conn Id | Type | Host | Port | Schema | Usado por |
|---------|------|------|------|--------|-----------|
| `hydra_postgres` | postgres | `192.168.1.96` | `5432` | `postgres` | Acesso a BD principal |
| `hydra_postgres_csv` | postgres | `192.168.1.96` | `5434` | `postgres` | DAG `exemplo_etl` (escrita metricas) |
| `api_tabular_conn` | http | `192.168.1.96` | `8006` | — | Referencia a API Metrics |
| `mongo_default` | mongo | `192.168.1.96` | `27017` | — | DAG `exemplo_etl` (logs) |
| `udata_http` | http | `192.168.1.96` | `7000` | — | Referencia (nao usado no DAG actual) |

### Verificar todas as conexoes

```bash
docker exec <container> airflow connections list -o table
```

## 5. Indices e constraints no PostgreSQL

O DAG `exemplo_etl` usa UPSERT nas tabelas do PostgreSQL CSV. E necessario criar os seguintes indices:

```bash
# Indice unico para upsert na tabela datasets
docker exec hydra-pt-database-csv-1 psql -U postgres -c \
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_datasets_unique ON datasets (dataset_id, metric_month);"

# Indice unico para upsert na tabela site
docker exec hydra-pt-database-csv-1 psql -U postgres -c \
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_site_metric_month_unique ON site (metric_month);"
```

### Verificar que as tabelas existem

```bash
docker exec hydra-pt-database-csv-1 psql -U postgres -c "\dt datasets; \dt datasets_total; \dt site;"
```

A tabela `datasets_total` e uma **view** que agrega os dados da tabela `datasets`:

```sql
-- View automatica (ja existe)
SELECT dataset_id,
       sum(monthly_visit) AS visit,
       sum(monthly_download_resource) AS download_resource,
       max(id) AS __id
FROM datasets
GROUP BY dataset_id;
```

## 6. Variaveis (Variables)

```bash
docker exec <container> airflow variables set UDATA_INSTANCE_URL "http://192.168.1.96:7000"
docker exec <container> airflow variables set METRICS_API_URL "http://192.168.1.96:8006/api"
docker exec <container> airflow variables set MONGODB_CONN_ID "mongo_default"
```

**Nota:** O DAG `exemplo_etl` actual usa constantes no codigo em vez de Variables, para simplicidade.

## 7. DAG exemplo_etl — Pipeline de metricas

### Fluxo

```
extract_from_udata → send_to_metrics_db → update_udata_metrics → save_to_mongodb
```

### Tasks

| Task | O que faz | Origem | Destino |
|------|-----------|--------|---------|
| `extract_from_udata` | Extrai metricas de datasets e contagens do site | MongoDB `udata.dataset` | XCom |
| `send_to_metrics_db` | Escreve metricas no PostgreSQL para a API Metrics | XCom | PostgreSQL `datasets` (porta 5434) |
| `update_udata_metrics` | Le metricas agregadas do PostgREST e escreve de volta no MongoDB do udata | API Metrics `datasets_total` | MongoDB `udata.dataset[].metrics` + `udata.metrics` |
| `save_to_mongodb` | Regista log do processo ETL | XCom | MongoDB `hydra_metrics.metrics_logs` |

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
UDATA_MONGO_HOST = "192.168.1.96"   # IP da maquina host
UDATA_MONGO_PORT = 27017
UDATA_MONGO_DB = "udata"             # Base de dados MongoDB do udata
METRICS_PG_CONN_ID = "hydra_postgres_csv"  # Airflow Connection ID
METRICS_MONGO_DB = "hydra_metrics"    # BD para logs do ETL
METRICS_API_URL = "http://192.168.1.96:8006/api"  # API Metrics (read)
```

### MongoHook — API correcta (v4.2.2)

O provider `apache-airflow-providers-mongo==4.2.2` usa `mongo_collection` como primeiro argumento:

```python
# CORRECTO
hook.insert_one(mongo_collection="metrics_logs", doc=log_doc, mongo_db="hydra_metrics")

# ERRADO (versoes anteriores)
hook.insert_one(collection="metrics_logs", doc=log_doc, mongo_db="hydra_metrics")
```

## 8. Arquitetura do fluxo de metricas

```
                    DAG exemplo_etl (Airflow)
                    ========================

  [1] MongoDB udata          [2] PostgreSQL CSV (5434)
      (porta 27017)               tabela: datasets
      +-----------------+         +------------------+
      | udata.dataset   |--extract-->| dataset_id     |
      |   .metrics      |         | metric_month     |
      |   .views        |         | monthly_visit    |
      |   .downloads    |         | monthly_download |
      +-----------------+         +------------------+
             ^                           |
             |                    [view: datasets_total]
             |                           |
             |                    +------------------+
             +---update_udata----| API Metrics:8006 |
                  _metrics       | /api/datasets_   |
                                 |   total/data/    |
                                 +------------------+

  [3] MongoDB udata           [4] MongoDB hydra_metrics
      (porta 27017)               (porta 27017)
      +-----------------+         +------------------+
      | udata.metrics   |         | metrics_logs     |
      | (site diario)   |         | (logs ETL)       |
      +-----------------+         +------------------+
```

### Como o udata le as metricas

O udata tem um job celery `update-metrics` (plugin `udata_metrics`) que:
1. Le de `{METRICS_API}/datasets_total/data/?visit__greater=1`
2. Atualiza `udata.dataset[].metrics.views` e `.resources_downloads`

**O celery nao esta a correr**, pelo que o DAG faz este trabalho na task `update_udata_metrics`.

Configuracao relevante no udata (`udata.cfg` / `.env`):

```
METRICS_API=http://localhost:8006/api
```

### Tabelas no PostgreSQL CSV (porta 5434)

| Tabela/View | Tipo | Descricao |
|------------|------|-----------|
| `datasets` | tabela | Metricas mensais por dataset (escrita pelo DAG) |
| `datasets_total` | view | Agregacao de `datasets` — soma total por dataset_id |
| `site` | tabela | Metricas mensais do site (visitas, contagens) |
| `organizations` | tabela | Metricas por organizacao |
| `reuses` | tabela | Metricas por reutilizacao |
| `resources` | tabela | Metricas por recurso |
| `views` | tabela | Views de paginas |
| `downloads` | tabela | Downloads de recursos |

### Collections no MongoDB

| Database | Collection | Descricao |
|----------|-----------|-----------|
| `udata` | `dataset` | Datasets com metricas embebidas em `metrics.*` |
| `udata` | `metrics` | Metricas diarias do site (formato: `{date, level, object_id, values}`) |
| `udata` | `metric_event` | Eventos de tracking (api_call, download) |
| `hydra_metrics` | `metrics_logs` | Logs de execucao do DAG |

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
c = MongoClient('192.168.1.96', 27017)
print(c.list_database_names())
"

# PostgreSQL
docker exec <container> python3 -c "
from airflow.providers.postgres.hooks.postgres import PostgresHook
hook = PostgresHook(postgres_conn_id='hydra_postgres_csv')
conn = hook.get_conn()
cur = conn.cursor()
cur.execute('SELECT count(*) FROM datasets')
print('datasets:', cur.fetchone()[0])
"

# API Metrics
docker exec <container> python3 -c "
import requests
r = requests.get('http://192.168.1.96:8006/health/')
print(r.json())
"
```

### Logs do DAG

```bash
# Logs do ultimo run
docker compose logs --tail=50 webserver | grep exemplo_etl

# Log de uma task especifica
docker exec <container> airflow tasks test exemplo_etl extract_from_udata 2026-03-18
```

### Reconstruir a imagem apos alteracoes

```bash
docker compose build --no-cache webserver
docker compose up -d
```
