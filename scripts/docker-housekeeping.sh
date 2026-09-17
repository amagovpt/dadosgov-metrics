#!/usr/bin/env bash
# Housekeeping do Docker: build cache e imagens órfãs.
#
# CORRE NO HOST, não no container. Está em ./scripts porque é a diretoria
# versionada onde já vive o prune-logs.sh, mas ao contrário desse não é montada
# para dentro do Airflow nem invocada por nenhuma DAG — o container não tem
# acesso ao /var/run/docker.sock (de propósito: dava-lhe controlo do daemon).
# O agendamento é o systemd timer 'docker-housekeeping.timer' (semanal), que
# aponta para este ficheiro; a unit no host é só um ponteiro, a lógica é esta.
#
# O que cresce de facto no /var/lib/docker não são os logs de stdout — o cap
# json-file (50m x 5) dos composes funciona e a 2026-09-17 os
# /var/lib/docker/containers somavam 90 MB. O que cresce é:
#
#   - BUILD CACHE: 6,2 GB reportados pelo 'docker system df' a 2026-09-17; o
#     prune libertou 9,7 GB, porque conta também camadas partilhadas com as
#     imagens órfãs. Acumula a cada build e nada o limpa. É reconstruído on
#     demand, por isso apagá-lo só custa tempo no build seguinte.
#   - IMAGENS ÓRFÃS: 3,8 GB em 34 imagens sem tag a 2026-09-17. Cada rebuild de
#     uma imagem local deixa a anterior sem tag.
#
# Deliberadamente NÃO faz duas coisas:
#
#   - 'docker image prune -a' (em vez de -f): o -a apaga também imagens COM tag
#     que não estejam em uso, obrigando a re-pull/rebuild. Não vale o risco de
#     deixar o host sem imagens base numa altura sem rede.
#   - 'docker volume prune' / 'system prune --volumes': os volumes
#     'hydra_database-data-15' e 'hydra_database-data-csv-15' aparecem como
#     dangling mas contêm dados Postgres, aparentemente de antes do rename do
#     projeto hydra -> hydra-pt. Apagá-los é irreversível.
set -eu

echo "[docker-housekeeping] $(date '+%F %T')"
docker system df

echo "[docker-housekeeping] build cache..."
docker builder prune -af

echo "[docker-housekeeping] imagens órfãs (só sem tag)..."
docker image prune -f

echo "[docker-housekeeping] estado final:"
docker system df
df -h / | tail -1
echo "[docker-housekeeping] concluído."
