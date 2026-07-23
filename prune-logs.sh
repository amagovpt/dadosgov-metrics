#!/usr/bin/env bash
# Limpeza dos logs do Airflow do dadosgov-metrics.
#
# Os logs (task logs em dag_id=*/, scheduler/, dag_processor_manager/) acumulam
# em ./logs, que é bind-mount do container (ver docker-compose.yml). As env vars
# AIRFLOW__LOGGING__LOG_RETENTION_DAYS / AIRFLOW__LOGGING__LOG_CLEANUP_ENABLED
# definidas no compose NÃO são chaves nativas do Airflow e não têm efeito — a
# retenção real é feita por este script, agendado pelo systemd timer
# 'dadosgov-metrics-logclean.timer'.
#
# São milhares de ficheiros pequenos numa árvore de diretórios, por isso usa-se
# find -mtime -delete (o logrotate, orientado a ficheiros nomeados, não serve).
set -eu

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
