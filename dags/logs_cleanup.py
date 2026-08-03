"""Limpeza diária dos logs do Airflow.

Os task logs escritos em /opt/airflow/logs (bind-mount de ./logs no host) não são
cobertos por nenhum mecanismo nativo: o cap 'max-size/max-file' do compose aplica-se
apenas aos logs de stdout no driver json-file. A remoção é feita por
scripts/prune-logs.sh, montado no container em /opt/airflow/scripts.

Esta DAG substitui o systemd timer 'dadosgov-metrics-logclean.timer' que fazia o
agendamento no host, fora do repositório.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

PRUNE_SCRIPT = "/opt/airflow/scripts/prune-logs.sh"
LOG_DIR = "/opt/airflow/logs"
RETENTION_DAYS = 3

with DAG(
    dag_id="logs_cleanup",
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
        "execution_timeout": timedelta(minutes=15),
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
        },
        append_env=True,
    )
