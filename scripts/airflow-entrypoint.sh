#!/usr/bin/env bash


#airflow resetdb
#airflow db init
#airflow upgradedb

# airflow db reset -y (removed to avoid confirmation prompt blocking startup)
airflow db migrate
airflow users create -r Admin -u "$AIRFLOW_ADMIN_MAIL" -e "$AIRFLOW_ADMIN_MAIL" -f "$AIRFLOW_ADMIN_FIRSTNAME" -l "$AIRFLOW_ADMIN_NAME" -p "$AIRFLOW_ADMIN_PASSWORD"
# Scheduler e webserver correm ambos neste container (LocalExecutor). Antes o
# scheduler ia para background sem supervisao e o webserver ficava em foreground:
# se o scheduler morresse, o container continuava vivo e nada era agendado.
#
# Agora ambos ficam em background e o 'wait -n' devolve assim que o PRIMEIRO
# terminar; sair com codigo != 0 faz o 'restart: on-failure' do compose repor o
# container com os dois processos. Um 'docker stop' explicito nao dispara a
# restart policy, por isso nao ha risco de loop no shutdown.
trap 'kill -TERM $(jobs -p) 2>/dev/null' TERM INT

airflow scheduler &
airflow webserver &

wait -n
echo "[entrypoint] scheduler ou webserver terminou; a encerrar o container para forcar restart" >&2
exit 1
