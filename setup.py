#!/usr/bin/env python3
"""
Setup interativo do Data Engineering Stack (Airflow + Metrics ETL).

Automatiza todos os passos descritos em docs/airflow-configuracao.md:
  1. Configuracao de IPs e portas (interativo)
  2. Configuracao do ficheiro .env
  3. Build e arranque dos containers (docker compose)
  4. Criacao das Airflow Connections
  5. Criacao das Airflow Variables
  6. Criacao dos indices PostgreSQL para UPSERT
  7. Atualizacao das constantes do DAG
  8. Verificacao de conectividade
  9. Trigger do DAG metrics_etl

Uso (executar na raiz do repositorio):
  python3 setup.py
"""

import os
import re
import shutil
import subprocess
import sys
import time

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_IPS = {
    "postgresql": "10.55.37.145",
    "mongodb": "10.55.37.143",
    "api_metrics": "10.55.37.145",
    "udata": "172.31.204.12",
}

DEFAULT_PORTS = {
    "postgresql": 5432,
    "postgresql_csv": 5434,
    "mongodb": 27017,
    "api_metrics": 8006,
    "postgrest": 8080,
    "udata": 7000,
}

DEFAULT_PG_USER = "postgres"
DEFAULT_PG_PASS = "postgres"
DEFAULT_PG_DB = "postgres"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def banner(msg):
    width = max(len(msg) + 4, 50)
    print(f"\n{'=' * width}")
    print(f"  {msg}")
    print(f"{'=' * width}\n")


def step(msg):
    print(f"\n--- {msg}")


def run(cmd, check=True, capture=False, **kwargs):
    """Run a shell command, printing it first."""
    if isinstance(cmd, str):
        print(f"  $ {cmd}")
    else:
        print(f"  $ {' '.join(cmd)}")
    return subprocess.run(
        cmd, shell=isinstance(cmd, str), check=check,
        capture_output=capture, text=True, **kwargs,
    )


def ask(prompt, default=""):
    """Prompt user with optional default."""
    suffix = f" [{default}]" if default else ""
    value = input(f"  {prompt}{suffix}: ").strip()
    return value if value else default


def ask_ip(label, default):
    """Ask for an IP address with basic validation."""
    while True:
        ip = ask(f"IP do {label}", default)
        if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip):
            return ip
        print(f"    IP invalido: {ip}. Tente novamente.")


def ask_port(label, default):
    """Ask for a port number."""
    while True:
        port = ask(f"Porta do {label}", str(default))
        if port.isdigit() and 1 <= int(port) <= 65535:
            return int(port)
        print(f"    Porta invalida: {port}. Tente novamente.")


def container_name(env_type, env_name):
    return f"airflow-{env_type}-{env_name}"


def docker_exec(container, cmd):
    """Run a command inside the Airflow container."""
    full = f'docker exec {container} {cmd}'
    return run(full, check=False, capture=True)


def wait_for_airflow(container, timeout=120):
    """Wait until Airflow webserver is healthy."""
    step(f"Aguardando Airflow ficar pronto (max {timeout}s)...")
    start = time.time()
    while time.time() - start < timeout:
        r = docker_exec(container, "airflow version")
        if r.returncode == 0:
            print(f"  Airflow pronto: {r.stdout.strip()}")
            return True
        time.sleep(5)
        remaining = int(timeout - (time.time() - start))
        print(f"  Aguardando... ({remaining}s restantes)")
    print("  AVISO: Timeout ao aguardar Airflow. Continuando...")
    return False


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------
def step_collect_ips():
    banner("1. Configuracao de IPs e portas")
    print("  Introduza os IPs dos servicos. Pressione Enter para usar o default.\n")

    cfg = {}
    cfg["pg_ip"] = ask_ip("PostgreSQL (Hydra)", DEFAULT_IPS["postgresql"])
    cfg["pg_port"] = ask_port("PostgreSQL principal", DEFAULT_PORTS["postgresql"])
    cfg["pg_csv_port"] = ask_port("PostgreSQL CSV", DEFAULT_PORTS["postgresql_csv"])
    cfg["pg_user"] = ask("Utilizador PostgreSQL", DEFAULT_PG_USER)
    cfg["pg_pass"] = ask("Password PostgreSQL", DEFAULT_PG_PASS)
    cfg["pg_db"] = ask("Base de dados PostgreSQL", DEFAULT_PG_DB)

    print()
    cfg["mongo_ip"] = ask_ip("MongoDB (udata)", DEFAULT_IPS["mongodb"])
    cfg["mongo_port"] = ask_port("MongoDB", DEFAULT_PORTS["mongodb"])

    print()
    cfg["api_ip"] = ask_ip("API Metrics / PostgREST", DEFAULT_IPS["api_metrics"])
    cfg["api_port"] = ask_port("API Metrics", DEFAULT_PORTS["api_metrics"])

    print()
    cfg["udata_ip"] = ask_ip("udata API", DEFAULT_IPS["udata"])
    cfg["udata_port"] = ask_port("udata API", DEFAULT_PORTS["udata"])

    return cfg


def step_env(repo_dir):
    banner("2. Configuracao do ficheiro .env")
    env_path = os.path.join(repo_dir, ".env")
    env_example = os.path.join(repo_dir, ".envExample")

    if os.path.isfile(env_path):
        print(f"  Ficheiro .env ja existe em {env_path}")
        overwrite = ask("Deseja recriar a partir do .envExample? (s/N)", "N")
        if overwrite.lower() != "s":
            return
    elif os.path.isfile(env_example):
        shutil.copy(env_example, env_path)
        print(f"  .envExample copiado para .env")
    else:
        print("  AVISO: .envExample nao encontrado. Crie o .env manualmente.")
        return

    print("\n  Preencha as variaveis do .env:\n")
    replacements = {
        "POSTGRES_USER": ask("POSTGRES_USER", DEFAULT_PG_USER),
        "POSTGRES_PASSWORD": ask("POSTGRES_PASSWORD", DEFAULT_PG_PASS),
        "POSTGRES_DB": ask("POSTGRES_DB", DEFAULT_PG_DB),
        "AIRFLOW_ADMIN_MAIL": ask("AIRFLOW_ADMIN_MAIL", ""),
        "AIRFLOW_ADMIN_FIRSTNAME": ask("AIRFLOW_ADMIN_FIRSTNAME", ""),
        "AIRFLOW_ADMIN_NAME": ask("AIRFLOW_ADMIN_NAME", ""),
        "AIRFLOW_ADMIN_PASSWORD": ask("AIRFLOW_ADMIN_PASSWORD", ""),
    }

    with open(env_path, "r") as f:
        content = f.read()

    for key, value in replacements.items():
        if value:
            content = re.sub(
                rf"^{key}=.*$",
                f"{key}={value}",
                content,
                flags=re.MULTILINE,
            )

    with open(env_path, "w") as f:
        f.write(content)

    print(f"\n  .env atualizado em {env_path}")


def step_docker_build(repo_dir):
    banner("3. Build e arranque dos containers")

    step("Build da imagem Airflow...")
    run(f"docker compose build --no-cache webserver", cwd=repo_dir)

    step("Arranque dos containers...")
    run(f"docker compose up -d", cwd=repo_dir)

    step("Estado dos containers:")
    run(f"docker compose ps", cwd=repo_dir)


def step_connections(container, cfg):
    banner("4. Criacao das Airflow Connections")

    connections = [
        {
            "id": "hydra_postgres",
            "type": "postgres",
            "host": cfg["pg_ip"],
            "port": cfg["pg_port"],
            "login": cfg["pg_user"],
            "password": cfg["pg_pass"],
            "schema": cfg["pg_db"],
            "desc": "Hydra main PostgreSQL database",
        },
        {
            "id": "hydra_postgres_csv",
            "type": "postgres",
            "host": cfg["pg_ip"],
            "port": cfg["pg_csv_port"],
            "login": cfg["pg_user"],
            "password": cfg["pg_pass"],
            "schema": cfg["pg_db"],
            "desc": "Hydra CSV PostgreSQL database (used by api-tabular/PostgREST)",
        },
        {
            "id": "api_tabular_conn",
            "type": "http",
            "host": cfg["api_ip"],
            "port": cfg["api_port"],
            "desc": "API Tabular (PostgREST)",
        },
        {
            "id": "mongo_default",
            "type": "mongo",
            "host": cfg["mongo_ip"],
            "port": cfg["mongo_port"],
            "desc": "MongoDB udata (sem autenticacao)",
        },
        {
            "id": "udata_http",
            "type": "http",
            "host": cfg["udata_ip"],
            "port": cfg["udata_port"],
            "desc": "udata API",
        },
    ]

    for conn in connections:
        step(f"Connection: {conn['id']}")

        # Delete existing (ignore errors)
        docker_exec(container, f'airflow connections delete "{conn["id"]}"')

        cmd = (
            f'airflow connections add "{conn["id"]}"'
            f' --conn-type {conn["type"]}'
            f' --conn-host "{conn["host"]}"'
            f' --conn-port {conn["port"]}'
        )
        if conn.get("login"):
            cmd += f' --conn-login "{conn["login"]}"'
        if conn.get("password"):
            cmd += f' --conn-password "{conn["password"]}"'
        if conn.get("schema"):
            cmd += f' --conn-schema "{conn["schema"]}"'
        cmd += f' --conn-description "{conn["desc"]}"'

        r = docker_exec(container, cmd)
        if r.returncode == 0:
            print(f"    OK")
        else:
            print(f"    ERRO: {r.stderr.strip()}")

    step("Verificacao de conexoes:")
    r = docker_exec(container, "airflow connections list -o table")
    if r.returncode == 0:
        for line in r.stdout.splitlines():
            if "hydra" in line or "mongo" in line or "tabular" in line or "udata" in line:
                print(f"    {line.strip()}")


def step_variables(container, cfg):
    banner("5. Criacao das Airflow Variables")

    variables = {
        "UDATA_INSTANCE_URL": f"http://{cfg['udata_ip']}:{cfg['udata_port']}",
        "METRICS_API_URL": f"http://{cfg['api_ip']}:{cfg['api_port']}/api",
        "MONGODB_CONN_ID": "mongo_default",
    }

    for key, value in variables.items():
        step(f"Variable: {key} = {value}")
        r = docker_exec(container, f'airflow variables set {key} "{value}"')
        if r.returncode == 0:
            print(f"    OK")
        else:
            print(f"    ERRO: {r.stderr.strip()}")


def step_pg_indexes(container, cfg):
    banner("6. Indices PostgreSQL para UPSERT")

    indexes = [
        (
            "visits_datasets_upsert_idx",
            "CREATE UNIQUE INDEX IF NOT EXISTS visits_datasets_upsert_idx "
            "ON metric.visits_datasets (dataset_id, date_metric);",
        ),
        (
            "visits_resources_upsert_idx",
            "CREATE UNIQUE INDEX IF NOT EXISTS visits_resources_upsert_idx "
            "ON metric.visits_resources (resource_id, date_metric);",
        ),
    ]

    pg_container = "hydra-pt-database-csv-1"

    # Check if pg container exists; if not, use psql from Airflow container
    check = run(f"docker inspect {pg_container}", check=False, capture=True)
    if check.returncode != 0:
        print(f"  Container '{pg_container}' nao encontrado.")
        print(f"  A criar indices via Airflow container com psycopg2...")
        for name, sql in indexes:
            step(f"Indice: {name}")
            py_cmd = (
                f"python3 -c \""
                f"import psycopg2; "
                f"c=psycopg2.connect(host='{cfg['pg_ip']}',port={cfg['pg_csv_port']},"
                f"user='{cfg['pg_user']}',password='{cfg['pg_pass']}',dbname='{cfg['pg_db']}'); "
                f"cur=c.cursor(); cur.execute('{sql}'); c.commit(); "
                f"print('OK'); c.close()\""
            )
            r = docker_exec(container, py_cmd)
            if r.returncode == 0:
                print(f"    {r.stdout.strip()}")
            else:
                print(f"    ERRO: {r.stderr.strip()}")
        return

    for name, sql in indexes:
        step(f"Indice: {name}")
        r = run(
            f'docker exec {pg_container} psql -U {cfg["pg_user"]} -c "{sql}"',
            check=False, capture=True,
        )
        if r.returncode == 0:
            print(f"    {r.stdout.strip()}")
        else:
            print(f"    ERRO: {r.stderr.strip()}")


def step_update_dag_constants(repo_dir, cfg):
    banner("7. Atualizacao das constantes do DAG")

    dag_path = os.path.join(repo_dir, "dags", "metrics_etl.py")
    if not os.path.isfile(dag_path):
        print(f"  DAG nao encontrado em {dag_path}. A saltar.")
        return

    with open(dag_path, "r") as f:
        content = f.read()

    replacements = {
        r'UDATA_MONGO_HOST\s*=\s*"[^"]*"':
            f'UDATA_MONGO_HOST = "{cfg["mongo_ip"]}"',
        r'UDATA_MONGO_PORT\s*=\s*\d+':
            f'UDATA_MONGO_PORT = {cfg["mongo_port"]}',
        r'METRICS_API_URL\s*=\s*"[^"]*"':
            f'METRICS_API_URL = "http://{cfg["api_ip"]}:{cfg["api_port"]}/api"',
    }

    updated = False
    for pattern, replacement in replacements.items():
        new_content = re.sub(pattern, replacement, content)
        if new_content != content:
            content = new_content
            updated = True

    if updated:
        with open(dag_path, "w") as f:
            f.write(content)
        print(f"  Constantes atualizadas em {dag_path}")
    else:
        print(f"  Constantes ja estavam corretas.")


def step_verify(container, cfg):
    banner("8. Verificacao de conectividade")

    checks = [
        (
            "MongoDB",
            f"python3 -c \""
            f"from pymongo import MongoClient; "
            f"c=MongoClient('{cfg['mongo_ip']}', {cfg['mongo_port']}); "
            f"print('OK -', c.list_database_names()); c.close()\"",
        ),
        (
            "PostgreSQL CSV (schema metric)",
            f"python3 -c \""
            f"import psycopg2; "
            f"c=psycopg2.connect(host='{cfg['pg_ip']}',port={cfg['pg_csv_port']},"
            f"user='{cfg['pg_user']}',password='{cfg['pg_pass']}',dbname='{cfg['pg_db']}'); "
            f"cur=c.cursor(); cur.execute('SELECT count(*) FROM metric.visits_datasets'); "
            f"print('OK - visits_datasets:', cur.fetchone()[0]); c.close()\"",
        ),
        (
            "API Metrics",
            f"python3 -c \""
            f"import requests; "
            f"r=requests.get('http://{cfg['api_ip']}:{cfg['api_port']}/api/datasets_total/data/?page_size=1', timeout=5); "
            f"print('OK -', r.status_code, r.json().get('meta', {{}}))\"",
        ),
    ]

    for name, cmd in checks:
        step(f"Teste: {name}")
        r = docker_exec(container, cmd)
        if r.returncode == 0:
            print(f"    {r.stdout.strip()}")
        else:
            err = r.stderr.strip().split("\n")[-1] if r.stderr else "unknown"
            print(f"    FALHOU: {err}")


def step_trigger_dag(container):
    banner("9. Trigger do DAG metrics_etl")

    trigger = ask("Deseja fazer trigger do DAG metrics_etl agora? (S/n)", "S")
    if trigger.lower() == "n":
        print("  A saltar trigger.")
        return

    step("Unpause do DAG...")
    docker_exec(container, "airflow dags unpause metrics_etl")

    step("Trigger manual...")
    r = docker_exec(container, "airflow dags trigger metrics_etl")
    if r.returncode == 0:
        print("  DAG triggered com sucesso.")
    else:
        print(f"  ERRO: {r.stderr.strip()}")

    print("\n  Para acompanhar a execucao:")
    print(f"    docker exec {container} airflow dags list-runs -d metrics_etl -o table")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    banner("Setup do Data Engineering Stack")
    print("  Este script configura o ambiente Airflow completo.")
    print("  Baseado em: docs/airflow-configuracao.md\n")

    # Determine repo root (where this script lives)
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(repo_dir)
    print(f"  Repositorio: {repo_dir}")

    # Check prerequisites
    for tool in ["docker"]:
        if not shutil.which(tool):
            print(f"  ERRO: '{tool}' nao encontrado. Instale-o e tente novamente.")
            sys.exit(1)

    r = run("docker compose version", check=False, capture=True)
    if r.returncode != 0:
        print("  ERRO: 'docker compose' nao disponivel.")
        sys.exit(1)

    # Step 1: Collect IPs
    cfg = step_collect_ips()

    # Step 2: .env
    step_env(repo_dir)

    # Read env to get container name
    env_type = "demo"
    env_name = "test"
    env_path = os.path.join(repo_dir, ".env")
    if os.path.isfile(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("AIRFLOW_ENV_TYPE="):
                    env_type = line.split("=", 1)[1]
                elif line.startswith("AIRFLOW_ENV_NAME="):
                    env_name = line.split("=", 1)[1]
    cname = container_name(env_type, env_name)
    print(f"\n  Nome do container Airflow: {cname}")

    # Step 3: Docker build & up
    step_docker_build(repo_dir)

    # Wait for Airflow
    wait_for_airflow(cname, timeout=120)

    # Step 4: Connections
    step_connections(cname, cfg)

    # Step 5: Variables
    step_variables(cname, cfg)

    # Step 6: PG indexes
    step_pg_indexes(cname, cfg)

    # Step 7: Update DAG constants
    step_update_dag_constants(repo_dir, cfg)

    # Step 8: Verify
    step_verify(cname, cfg)

    # Step 9: Trigger
    step_trigger_dag(cname)

    banner("Setup concluido!")
    webserver_port = "28080"
    if os.path.isfile(env_path):
        with open(env_path) as f:
            for line in f:
                if line.strip().startswith("AIRFLOW_WEBSERVER_PORT="):
                    webserver_port = line.strip().split("=", 1)[1]
    print(f"  Airflow UI: http://localhost:{webserver_port}")
    print(f"  Container:  {cname}")
    print(f"  DAG:        metrics_etl")
    print(f"  Docs:       docs/airflow-configuracao.md\n")


if __name__ == "__main__":
    main()
