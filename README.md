# Infrastructure Airflow

Ce repository a pour objectif de mettre en place rapidement une infrastructure Airflow permettant à chacun de tester son DAG avant mise en production.

L'infrastructure actuelle est basée sur du LocalExecutor (le scheduler, le webserver et worker sont hébergés sur le même container)

## Installation

```
git clone git@github.com:etalab/data-engineering-stack.git
cd data-engineering-stack

# Create directories necessary for Airflow to work
./1_prepareDirs.sh

# Prepare .env file
./2_prepare_env.sh
nano .env
# Edit POSTGRES_USER ; POSTGRES_PASSWORD ; POSTGRES_DB ; AIRFLOW_ADMIN_MAIL ; AIRFLOW_ADMIN_FIRSTNAME ; AIRFLOW_ADMIN_NAME ; AIRFLOW_ADMIN_PASSWORD

# For MacOS with ARM:
# export DOCKER_DEFAULT_PLATFORM="linux/amd64"

# Launch services
docker-compose up --build -d

# After few seconds, you can connect to http://localhost:8080 with login : AIRFLOW_ADMIN_MAIL and password : AIRFLOW_ADMIN_PASSWORD
```

## Refresh dags

```
# Airflow used to have a little time before dag refreshing when dag is created. You can force refreshing with :
./refreshBagDags.sh
```

## Connections

Connections can be created manually or with python scripts `createConn.py` (using Airflow API) inside each projects. You need also to add your ssh key inside `ssh` folder of repository for the container to be able to see it in `/home/airflow/.ssh/` folder of container.

## Limpeza dos logs

Os task logs escritos em `./logs` (bind-mount de `/opt/airflow/logs`) não são cobertos
por nenhum mecanismo nativo do Airflow nem pelo Docker: o bloco `logging:` do
`docker-compose.yml` (`max-size: 50m`, `max-file: 5`) limita apenas os logs de
stdout/stderr no driver `json-file`, não ficheiros dentro do bind-mount.

A remoção é feita pela DAG **`logs_cleanup`** (`dags/logs_cleanup.py`), diária às 03:00,
que invoca `scripts/prune-logs.sh` dentro do container:

| | |
|---|---|
| Retenção | `RETENTION_DAYS=3` (efetiva ~4 dias, ver nota abaixo) |
| Diretório | `/opt/airflow/logs` (via `AIRFLOW_LOG_DIR`) |
| Agendamento | `schedule="0 3 * * *"` |

A DAG tem `is_paused_upon_creation=False` para não nascer em pausa
(`airflow.cfg` tem `dags_are_paused_at_creation = True`), e o `setup.py` faz
`airflow dags unpause logs_cleanup` de forma incondicional. As duas coisas são
necessárias: o flag cobre ambientes criados sem correr o `setup.py`, o unpause
cobre o caso de a DAG já existir na metadata DB.

**Nota sobre a retenção:** `find -mtime +3` só apanha ficheiros com 4 dias completos,
por isso a retenção efetiva é ~4 dias, não 3. É normal ver ficheiros de 4 dias à
espera da passagem seguinte. Para reter mesmo 3 dias, usar `RETENTION_DAYS=2`.

Para correr a limpeza manualmente:

```
docker exec airflow-<env>-<name> airflow dags trigger logs_cleanup
```
# dadosgov-metrics
