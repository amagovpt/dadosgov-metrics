#!/usr/bin/env bash
# Limpeza dos logs do Airflow do dadosgov-metrics.
#
# Os logs (task logs em dag_id=*/, scheduler/, dag_processor_manager/) acumulam
# em ./logs, que é bind-mount do container (ver docker-compose.yml). O cap
# 'max-size/max-file' do compose só limita os logs de stdout no driver json-file;
# não tem qualquer efeito sobre estes ficheiros. As env vars
# AIRFLOW__LOGGING__LOG_RETENTION_DAYS / AIRFLOW__LOGGING__LOG_CLEANUP_ENABLED,
# que já existiram no .env, também NÃO são chaves nativas do Airflow — a
# retenção real é feita por este script.
#
# Agendamento: DAG 'logs_cleanup' (dags/logs_cleanup.py), que o invoca dentro do
# container com AIRFLOW_LOG_DIR=/opt/airflow/logs.
#
# São milhares de ficheiros pequenos numa árvore de diretórios, por isso usa-se
# find -mtime -delete (o logrotate, orientado a ficheiros nomeados, não serve).
# Nota: '-mtime +N' só apanha ficheiros com N+1 dias completos, por isso a
# retenção efetiva é ~RETENTION_DAYS+1 dias.
set -eu

# Default: caminho no host. Dentro do container passa-se AIRFLOW_LOG_DIR.
LOG_DIR="${AIRFLOW_LOG_DIR:-/opt/dadosgov-metrics/logs}"
RETENTION_DAYS="${AIRFLOW_LOG_RETENTION_DAYS:-3}"

if [ ! -d "$LOG_DIR" ]; then
  echo "[prune-logs] $LOG_DIR não existe; nada a fazer."
  exit 0
fi

echo "[prune-logs] $(date '+%F %T') a remover logs com mais de ${RETENTION_DAYS} dias em $LOG_DIR"

# 1) Apagar ficheiros mais antigos que RETENTION_DAYS dias.
find "$LOG_DIR" -type f -mtime +"$RETENTION_DAYS" -delete

# 2) Remover diretórios vazios deixados para trás (nunca a própria raiz).
#    -delete implica -depth, por isso remove primeiro os filhos e depois os pais
#    que ficaram vazios.
find "$LOG_DIR" -mindepth 1 -type d -empty -delete

echo "[prune-logs] concluído."
