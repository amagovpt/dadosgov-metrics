"""Manutenção diária do dadosgov-metrics: logs em disco e metadata do Airflow.

Duas fontes de crescimento sem limite, nenhuma delas coberta por mecanismos
nativos:

1) TASK LOGS em /opt/airflow/logs (bind-mount de ./logs no host). O cap
   'max-size/max-file' do compose aplica-se apenas aos logs de stdout no
   driver json-file. A remoção é feita por scripts/prune-logs.sh, montado no
   container em /opt/airflow/scripts.

2) METADATA em Postgres (./pg-airflow). O Airflow não apaga histórico por si;
   'airflow db clean' é o único mecanismo. A tabela crítica é o XCom: o
   retorno de metrics_etl.extract_tracking_events é o histórico completo do
   metric_event (~148 MB por run, 96 runs/dia). Sem limpeza chegou aos 12 GB
   em dois meses — ver NOTA no fim sobre a correção de raiz.

Esta DAG substituiu o systemd timer 'dadosgov-metrics-logclean.timer', que fazia
o agendamento no host, fora do repositório (removido a 2026-08-03). Chamava-se
'logs_cleanup' até passar a tratar também da metadata.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

PRUNE_SCRIPT = "/opt/airflow/scripts/prune-logs.sh"

# Caminho DENTRO do container (compose: ./logs:/opt/airflow/logs). Usar aqui o
# caminho do host faria o script cair no guard 'não existe; nada a fazer' e sair
# com código 0 — a DAG ficava verde sem limpar nada.
LOG_DIR = "/opt/airflow/logs"
RETENTION_DAYS = 3

# Tecto de espaço da árvore de logs, aplicado depois do critério de idade. É uma
# rede de segurança, não o mecanismo principal: com o volume normal (~2 MB/hora)
# 3 dias de logs ocupam ~150 MB, pelo que 1 GB só dispara numa anomalia.
#
# NOTA: o valor anterior era 250 MB, derivado do cap json-file dos composes
# (50 MB x 5). Era uma analogia errada — esse cap é por container e só para
# stdout, enquanto isto é o agregado dos task logs de todas as DAGs. Com um
# tecto tão baixo o critério de idade nunca chegava a aplicar-se: a retenção
# efetiva passou a ser o MIN_AGE_MIN (60 min) do script em vez dos 3 dias, e a
# limpeza de 2026-09-17 apagou 14 GB de logs com menos de um dia.
MAX_MB = 1024

# O XCom do metrics_etl são payloads consumidos pelas tasks a jusante do mesmo
# run; passado o run não têm valor operacional. 2 dias cobrem o diagnóstico de
# uma falha nocturna vista na manhã seguinte, e mantêm a tabela em ~5 GB.
XCOM_RETENTION_DAYS = 2

# Histórico de runs (grelha da UI, auditoria). Cresce devagar: log,
# task_instance e job somavam 80 MB em dois meses.
META_RETENTION_DAYS = 90

# 'celery_taskmeta'/'celery_tasksetmeta' ficam de fora: este ambiente usa
# LocalExecutor (AIRFLOW__CORE__EXECUTOR no .env — prevalece sobre o
# 'SequentialExecutor' do airflow.cfg), sem Celery, e essas tabelas não existem.
META_TABLES = (
    "log,job,task_instance,task_instance_history,task_fail,task_reschedule,"
    "sla_miss,dag_run,dataset_event,import_error,session"
)


# Texto do aviso com que airflow.utils.db_cleanup._suppress_with_logging engole
# OperationalError/ProgrammingError por tabela. Se mudar numa versão futura do
# Airflow, o wrapper abaixo deixa de apanhar falhas — verificar ao atualizar.
SUPPRESSED_ERROR = "Encountered error when attempting to clean table"


def _db_clean(tables: str, days: int) -> str:
    """Comando 'airflow db clean' com corte relativo ao intervalo do run.

    --skip-archive é obrigatório para libertar espaço: por omissão as linhas
    purgadas ficam em tabelas '_airflow_deleted__<tabela>__<ts>' e o disco não
    desce. Mas atenção: mesmo com o flag, o db clean faz primeiro um
    'CREATE TABLE ... AS SELECT' com todas as linhas a apagar, depois o DELETE,
    e só no fim o DROP — o pico de disco é ~2x o volume purgado. É por isso que
    o log mostra 'Moving data to table _airflow_deleted__...' e no fim a tabela
    não existe.

    O comando vem embrulhado porque o CLI sai com 0 mesmo quando falha: o
    db_cleanup envolve cada tabela em _suppress_with_logging, que apanha
    OperationalError/ProgrammingError, escreve só um warning e continua. Sem o
    wrapper, um 'No space left on device' no CTAS (precisamente o cenário que
    esta DAG existe para evitar) deixava a task verde e a tabela intacta.

    O DELETE não devolve espaço ao SO por si — o autovacuum apenas o marca como
    reutilizável, pelo que a tabela estabiliza em vez de encolher. O 'VACUUM
    FULL xcom' de 2026-09-17 recuperou ~5,6 GB. Ficaram ~6,3 GB de linhas vivas
    anteriores à correção do payload (223 linhas de 28 MB comprimidos), que só
    saem pela retenção a 2026-09-19 — aí um segundo VACUUM FULL pontual recupera
    esse resto. Depois disso, em regime permanente, não é preciso repetir.
    """
    cutoff = (
        "{{ (data_interval_end - macros.timedelta(days="
        + str(days)
        + ")).isoformat() }}"
    )
    clean = (
        "airflow db clean --yes --skip-archive "
        f"--tables {tables} "
        f'--clean-before-timestamp "{cutoff}"'
    )
    # Captura a saída, propaga um exit code != 0 do próprio CLI e, se este saiu
    # com 0 mas registou o aviso de erro suprimido, falha a task na mesma.
    return (
        f"out=$({clean} 2>&1); rc=$?; "
        "printf '%s\\n' \"$out\"; "
        '[ "$rc" -eq 0 ] || exit "$rc"; '
        f'if grep -q "{SUPPRESSED_ERROR}" <<<"$out"; then '
        'echo "[db_clean] o airflow db clean suprimiu um erro de BD (ver acima); '
        'a falhar a task" >&2; exit 1; fi'
    )


with DAG(
    dag_id="maintenance",
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    # Sem isto a DAG nasce em pausa (airflow.cfg: dags_are_paused_at_creation=True)
    # e a limpeza nunca corre num ambiente criado de raiz.
    is_paused_upon_creation=False,
    tags=["metrics", "maintenance"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=30),
    },
) as dag:
    limpar_logs = BashOperator(
        task_id="prune_logs",
        # O espaço final é obrigatório: um bash_command terminado em '.sh' é
        # interpretado pelo Jinja como caminho de template e falha a carregar.
        bash_command=f"{PRUNE_SCRIPT} ",
        env={
            "AIRFLOW_LOG_DIR": LOG_DIR,
            "AIRFLOW_LOG_RETENTION_DAYS": str(RETENTION_DAYS),
            "AIRFLOW_LOG_MAX_MB": str(MAX_MB),
        },
        append_env=True,
    )

    limpar_xcom = BashOperator(
        task_id="db_clean_xcom",
        bash_command=_db_clean("xcom", XCOM_RETENTION_DAYS),
    )

    limpar_metadata = BashOperator(
        task_id="db_clean_metadata",
        bash_command=_db_clean(META_TABLES, META_RETENTION_DAYS),
    )

    # Em série: com LocalExecutor as três podiam correr em paralelo, e não se
    # quer um 'db clean' a rodar ao mesmo tempo que o 'prune' nem dois 'db
    # clean' concorrentes. Encadear também deixa a ordem explícita na grelha.
    limpar_logs >> limpar_xcom >> limpar_metadata

# NOTA — a causa raiz foi corrigida a 2026-09-17:
# extract_tracking_events agregava o metric_event sem filtro de data, logo cada
# run re-extraía o histórico completo (147 MB/run, 96 runs/dia). Passou a ter
# uma janela nas agregações DIÁRIAS (metrics_etl.DAILY_WINDOW_DAYS, 14 dias por
# omissão) e o payload caiu para 7,2 MB. Os totais cumulativos continuam a
# varrer tudo, de propósito — janelá-los tornava-os "totais dos últimos N dias".
# Com isto o XCom estabiliza em algumas centenas de MB e esta limpeza deixa de
# ser contenção para passar a ser manutenção normal.
