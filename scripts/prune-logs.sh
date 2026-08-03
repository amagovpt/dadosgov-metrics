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
# Dois critérios, aplicados por esta ordem:
#
#   1) IDADE  — remove ficheiros com mais de RETENTION_DAYS dias.
#               Nota: '-mtime +N' só apanha ficheiros com N+1 dias completos, por
#               isso a retenção efetiva é ~RETENTION_DAYS+1 dias.
#
#   2) TAMANHO — se a árvore continuar acima de MAX_MB, remove os ficheiros mais
#               antigos até ficar abaixo. Este tecto padroniza a diretoria pelo
#               mesmo limite dos caps json-file dos composes (50 MB x 5 = 250 MB).
#               Sem ele, um pico de atividade dentro da janela de retenção podia
#               encher o disco antes de a idade chegar para limpar.
#
# São milhares de ficheiros pequenos numa árvore de diretórios, por isso usa-se
# find (o logrotate, orientado a ficheiros nomeados, não serve).
set -eu

# Default: caminho no host. Dentro do container passa-se AIRFLOW_LOG_DIR.
LOG_DIR="${AIRFLOW_LOG_DIR:-/opt/dadosgov-metrics/logs}"
RETENTION_DAYS="${AIRFLOW_LOG_RETENTION_DAYS:-3}"
MAX_MB="${AIRFLOW_LOG_MAX_MB:-250}"

# Ficheiros modificados nos últimos MIN_AGE_MIN minutos nunca são removidos pelo
# critério de tamanho: podem estar a ser escritos por tarefas em execução.
MIN_AGE_MIN="${AIRFLOW_LOG_MIN_AGE_MIN:-60}"

# Ficheiros removidos por volta do ciclo de tamanho (entre medições do 'du').
BATCH="${AIRFLOW_LOG_PRUNE_BATCH:-200}"

if [ ! -d "$LOG_DIR" ]; then
  echo "[prune-logs] $LOG_DIR não existe; nada a fazer."
  exit 0
fi

dir_size_kb() {
  du -sk "$LOG_DIR" | cut -f1
}

echo "[prune-logs] $(date '+%F %T') a limpar $LOG_DIR"
echo "[prune-logs] critérios: idade > ${RETENTION_DAYS} dias, tecto ${MAX_MB}MB"

# ---------------------------------------------------------------------------
# 1) Critério de idade
# ---------------------------------------------------------------------------
find "$LOG_DIR" -type f -mtime +"$RETENTION_DAYS" -delete

# ---------------------------------------------------------------------------
# 2) Critério de tamanho
# ---------------------------------------------------------------------------
max_kb=$((MAX_MB * 1024))
total_kb=$(dir_size_kb)

if [ "$total_kb" -le "$max_kb" ]; then
  echo "[prune-logs] tamanho ${total_kb}KB dentro do tecto (${max_kb}KB)."
else
  echo "[prune-logs] tamanho ${total_kb}KB acima do tecto (${max_kb}KB); a remover os mais antigos."

  # Remove por lotes, medindo o disco a cada volta. Não se pode prever o total
  # subtraindo o tamanho de cada ficheiro removido: os ficheiros protegidos pelo
  # -mmin (p. ex. dag_processor_manager.log, que é escrito continuamente e pode
  # ter dezenas de MB) contam para o 'du' mas nunca são removidos, pelo que um
  # acumulador nunca convergiria e o ciclo apagaria tudo o resto.
  while [ "$(dir_size_kb)" -gt "$max_kb" ]; do
    before=$(find "$LOG_DIR" -type f -mmin +"$MIN_AGE_MIN" | wc -l)
    [ "$before" -eq 0 ] && break

    find "$LOG_DIR" -type f -mmin +"$MIN_AGE_MIN" -printf '%T@\t%p\0' \
      | sort -zn \
      | head -z -n "$BATCH" \
      | cut -z -d"$(printf '\t')" -f2- \
      | xargs -0 -r rm -f

    # Se um lote não removeu nada, pára em vez de girar indefinidamente.
    [ "$(find "$LOG_DIR" -type f -mmin +"$MIN_AGE_MIN" | wc -l)" -eq "$before" ] && break
  done

  total_kb=$(dir_size_kb)
  if [ "$total_kb" -gt "$max_kb" ]; then
    echo "[prune-logs] AVISO: ainda em ${total_kb}KB. Os ficheiros restantes têm" \
         "menos de ${MIN_AGE_MIN} minutos e podem estar em uso; não foram removidos."
  else
    echo "[prune-logs] tamanho reduzido para ${total_kb}KB."
  fi
fi

# ---------------------------------------------------------------------------
# 3) Remover diretórios vazios deixados para trás (nunca a própria raiz).
#    -delete implica -depth, por isso remove primeiro os filhos e depois os pais
#    que ficaram vazios.
# ---------------------------------------------------------------------------
find "$LOG_DIR" -mindepth 1 -type d -empty -delete

echo "[prune-logs] concluído."
