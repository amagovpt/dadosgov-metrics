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

## Manutenção: logs e metadata

Os task logs escritos em `./logs` (bind-mount de `/opt/airflow/logs`) não são cobertos
por nenhum mecanismo nativo do Airflow nem pelo Docker: o bloco `logging:` do
`docker-compose.yml` (`max-size: 50m`, `max-file: 5`) limita apenas os logs de
stdout/stderr no driver `json-file`, não ficheiros dentro do bind-mount.

A remoção é feita pela DAG **`maintenance`** (`dags/maintenance.py`), diária às 03:00,
cuja task `prune_logs` invoca `scripts/prune-logs.sh` dentro do container:

| | |
|---|---|
| Critério 1 — idade | `RETENTION_DAYS=3` (efetiva ~4 dias, ver nota abaixo) |
| Critério 2 — tamanho | `MAX_MB=1024` (rede de segurança, ver nota abaixo) |
| Proteção | ficheiros com menos de `MIN_AGE_MIN=60` minutos nunca são removidos pelo tamanho |
| Diretório | `/opt/airflow/logs` (via `AIRFLOW_LOG_DIR`) |
| Agendamento | `schedule="0 3 * * *"` |

Os dois critérios aplicam-se por esta ordem: primeiro remove-se por idade; se a
árvore continuar acima do tecto, removem-se os ficheiros mais antigos até ficar
abaixo. O tecto existe para o caso de um pico de atividade encher o disco dentro
da janela de retenção, antes de a idade chegar para limpar.

**O tecto tem de ficar bem acima do volume normal de `RETENTION_DAYS`**, senão
passa a dominar e a retenção real cai para os 60 minutos do `MIN_AGE_MIN`. O
antigo default de 250 MB vinha do cap `json-file` dos composes, o que era uma
analogia errada — esse cap é por container e só para stdout, enquanto isto é o
agregado dos task logs de todas as DAGs. A 2026-09-17 essa afinação apagou 14 GB
de logs com menos de um dia sem o critério de idade chegar a aplicar-se.

A proteção dos 60 minutos evita apagar ficheiros que possam estar a ser escritos
por tarefas em execução. Se, por causa dela, o tecto não for atingido, o script
avisa em vez de forçar — é o que acontece quando um único ficheiro recente é
maior que o tecto (p. ex. `dag_processor_manager.log`, que o Airflow escreve
continuamente).

A DAG tem `is_paused_upon_creation=False` para não nascer em pausa
(`airflow.cfg` tem `dags_are_paused_at_creation = True`), e o `setup.py` faz
`airflow dags unpause maintenance` de forma incondicional. As duas coisas são
necessárias: o flag cobre ambientes criados sem correr o `setup.py`, o unpause
cobre o caso de a DAG já existir na metadata DB.

**Nota sobre a retenção:** `find -mtime +3` só apanha ficheiros com 4 dias completos,
por isso a retenção efetiva é ~4 dias, não 3. É normal ver ficheiros de 4 dias à
espera da passagem seguinte. Para reter mesmo 3 dias, usar `RETENTION_DAYS=2`.

### Metadata do Airflow (`./pg-airflow`)

O Airflow não apaga histórico por si, e `AIRFLOW__LOGGING__*` não tem nada a ver
com isto. As tasks `db_clean_xcom` e `db_clean_metadata` da mesma DAG correm
`airflow db clean`:

| | |
|---|---|
| `db_clean_xcom` | `XCOM_RETENTION_DAYS=2` |
| `db_clean_metadata` | `META_RETENTION_DAYS=90` (`log`, `job`, `task_instance`, `dag_run`, …) |

O XCom está separado porque é a tabela que cresce a sério: o retorno de
`metrics_etl.extract_tracking_events` é o histórico completo do `metric_event`
(~148 MB por run, 96 runs/dia), consumido pelas três tasks a jusante do mesmo
run. Sem limpeza chegou aos **12 GB em dois meses**.

Dois detalhes que não são opcionais:

- **`--skip-archive`.** Por omissão o `db clean` copia as linhas purgadas para
  tabelas `_airflow_deleted__<tabela>__<ts>` e o disco não desce.
- **O `DELETE` não devolve espaço ao SO.** O autovacuum apenas o marca como
  reutilizável, pelo que a tabela estabiliza em vez de encolher. O `VACUUM FULL
  xcom` de 2026-09-17 recuperou ~5,6 GB; ficaram ~6,3 GB de linhas vivas
  anteriores à correção do payload, que só saem pela retenção a 2026-09-19 — aí
  um segundo `VACUUM FULL` pontual recupera esse resto. Depois, em regime
  permanente, não é preciso repetir.

**Causa raiz — corrigida a 2026-09-17.** Ver a secção seguinte.

Para correr a limpeza manualmente:

```
docker exec airflow-<env>-<name> airflow dags trigger maintenance
```

### Janela das agregações diárias (`metrics_etl`)

O `extract_tracking_events` agregava o `metric_event` sem qualquer filtro de
data, por isso cada run re-extraía o histórico completo: **147,3 MB por run, 96
runs por dia**, dos quais 95% eram o mesmo bloco histórico re-enviado de cada
vez (689 184 entradas diárias, das quais só 88 nos últimos 14 dias).

As agregações são de duas classes e **só uma pode ser janelada**:

| classe | chaves | janelável? |
|---|---|---|
| Diárias | `dataset_views_daily`, `resource_downloads_daily` | **sim** |
| Totais cumulativos | `dataset_views`, `dataset_downloads`, `resource_downloads`, `org_views`, `reuse_views`, `dataservice_views`, `site_counts` | **não** |

Os totais vão para o MongoDB/udata como valores absolutos: filtrá-los por data
tornava-os "totais dos últimos N dias", ou seja valores errados. Por isso a
janela aplica-se apenas aos dois `$match` das diárias — e note-se que esses
`$match` são **strings idênticas** às das agregações cumulativas, pelo que
qualquer edição por substituição de texto tem de ser ancorada no comentário.

A janela é a `DAILY_WINDOW_DAYS` (14 dias), configurável pela Variable
`METRICS_DAILY_WINDOW_DAYS`. **`0` desliga o filtro** e corre sobre o histórico
completo, para um backfill de um ambiente novo.

Porque é seguro: os destinos fazem `INSERT ... ON CONFLICT (dataset_id,
date_metric) DO UPDATE` — UPSERT puro, sem `DELETE` nem `TRUNCATE` — pelo que
reprocessar só a janela recente é equivalente a reprocessar tudo, e as linhas
antigas ficam como estão. E têm de ficar: o `metric.visits_datasets` tinha
962 764 linhas desde **2018-07-24**, enquanto o `metric_event` no Mongo tem TTL
de **90 dias** (índice `created_at_1`, `expireAfterSeconds=7776000`). O Mongo é a
store de curto prazo, o Postgres é o armazém.

**O corte da janela é à meia-noite UTC**, nunca "agora − N dias" com hora. As
diárias agrupam por dia e o UPSERT faz `SET nb_visit = EXCLUDED.nb_visit`
(substitui). Com um corte a meio do dia, o dia da fronteira entrava só
parcialmente e o seu `nb_visit` era reescrito, a cada run de 15 min, com um
valor cada vez menor — um dia de histórico corrompido por dia, em silêncio.
Alinhado à meia-noite, cada dia está inteiro dentro ou inteiro fora da janela.
Foi um achado da auditoria de 2026-09-17: estava latente porque a fronteira caía
no intervalo sem eventos descrito abaixo.

14 dias em vez de 7 porque não custa nada — a atividade recente é pouca e as
duas janelas dão o mesmo payload — e dá margem para eventos que cheguem
atrasados.

> **Atenção ao interagir com a falha de ingestão descrita abaixo.** A janela
> tolera eventos atrasados até `DAILY_WINDOW_DAYS`. Se a ingestão for reparada e
> os eventos em falta forem reinjetados com o `created_at` original (anterior à
> janela), este ETL **não os apanha**. Nesse caso, correr um run com
> `METRICS_DAILY_WINDOW_DAYS=0` (histórico completo) e só depois voltar a 14.

#### Falha de ingestão do `metric_event` (aberta, 2026-08-24)

Detetada a 2026-09-17 ao investigar o `dataset_downloads`. Não é causada por
nada neste repositório — é a montante, na escrita dos eventos:

```
eventos entre 2026-08-25 e 2026-09-08 ....... 0   (15 dias sem nada)
media diaria 2026-08-18..08-24 ......... 68 613/dia
media diaria 2026-09-09..09-17 .............. 57/dia
```

O intervalo vazio está no **meio** da coleção (evento mais antigo 2026-06-19,
mais recente 2026-09-17), por isso não é o TTL a apagar a cauda — é uma
interrupção real, seguida de uma queda de ~1200× que continua. Enquanto isto não
for resolvido, as métricas de Setembro em diante são residuais, e nenhuma
limpeza ou janela o corrige: os dados não existem.

Sintoma visível no destino: `metric.visits_datasets` tem 350 021 linhas em
2026-08 e **135** em 2026-09.


Resultado, validado com as duas versões a correr consecutivamente:

```
payload:            147,3 MB -> 7,2 MB  (-95%)
dataset_views_daily   689 177 -> 88
resource_downloads..   306 064 -> 2
7 totais + total_events idênticos (igualdade profunda)
metric.visits_datasets  962 764 linhas, intactas
```

### Docker no host (build cache e imagens órfãs)

O que enche o `/var/lib/docker` não são os logs de stdout — o cap `json-file`
dos composes funciona (90 MB em `containers/` a 2026-09-17). São o **build
cache** e as **imagens sem tag**, que nada limpa: somavam **13,5 GB** a
2026-09-17.

Isto não pode correr numa DAG: o container do Airflow não tem acesso ao
`/var/run/docker.sock`, e montá-lo dar-lhe-ia controlo do daemon do host. Fica
em `scripts/docker-housekeeping.sh` (versionado) chamado por um systemd timer
semanal, que é a única peça no host:

```
/etc/systemd/system/docker-housekeeping.{service,timer}   # OnCalendar=Sun 04:00
  └─ ExecStart=/opt/dadosgov-metrics/scripts/docker-housekeeping.sh
```

```
# correr à mão
sudo systemctl start docker-housekeeping.service
journalctl -u docker-housekeeping.service -n 30
systemctl list-timers docker-housekeeping.timer
```

O script usa `docker image prune -f` (só imagens sem tag) e **nunca**
`docker volume prune`: os volumes `hydra_database-data-15` e
`hydra_database-data-csv-15` aparecem como *dangling* mas contêm dados
Postgres, aparentemente de antes do rename `hydra` → `hydra-pt`. O cabeçalho do
script explica as duas omissões.

O journal do host também não tinha limite (o default é 10% do sistema de
ficheiros, ≈8,7 GB); está limitado a 500 MB pelo drop-in
`/etc/systemd/journald.conf.d/99-maxuse.conf`.

## Supervisão do scheduler

Com LocalExecutor, o scheduler e o webserver correm no mesmo container. O
`scripts/airflow-entrypoint.sh` lança ambos em background e usa `wait -n`: se
qualquer um terminar, o entrypoint sai com código != 0 e o `restart: on-failure`
do compose repõe o container com os dois processos.

O healthcheck testa as duas coisas — o PID do webserver **e** `airflow jobs check
--job-type SchedulerJob`. Antes olhava apenas para o webserver, pelo que um
scheduler morto deixava o container `healthy`, com a UI a responder e nada a ser
agendado (nem o ETL, nem a limpeza de logs).
# dadosgov-metrics
