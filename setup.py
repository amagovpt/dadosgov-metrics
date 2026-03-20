#!/usr/bin/env python3
"""
Setup interativo do Data Engineering Stack (Airflow + Metrics ETL).

Automatiza todos os passos descritos em docs/airflow-configuracao.md:
  1. Build e arranque dos containers (docker compose)
  2. Importacao das Airflow Connections
  3. Importacao das Airflow Variables
  4. Criacao das tabelas no Hydra (PostgreSQL)
  5. Trigger do DAG metrics_etl

Uso (executar na raiz do repositorio):
  python3 setup.py
"""

import json
import os
import re
import shutil
import subprocess
import sys
import time


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
def step_prepare_dirs(repo_dir):
    banner("0. Preparacao dos diretorios")
    run("bash 1_prepareDirs.sh", cwd=repo_dir)


def step_prepare_env(repo_dir):
    env_path = os.path.join(repo_dir, ".env")
    if os.path.isfile(env_path):
        print("  .env ja existe. A saltar criacao.")
        return
    banner("0b. Criacao do ficheiro .env")
    run("bash 2_prepare_env.sh", cwd=repo_dir)


def step_docker_build(repo_dir):
    banner("1. Build e arranque dos containers")

    step("Build da imagem Airflow...")
    run(f"docker compose build --no-cache webserver", cwd=repo_dir)

    step("Arranque dos containers...")
    run(f"docker compose up -d", cwd=repo_dir)

    step("Estado dos containers:")
    run(f"docker compose ps", cwd=repo_dir)


def step_import_connections(repo_dir):
    banner("2. Importacao das Airflow Connections")

    connections_path = os.path.join(repo_dir, "docs", "connections.json")

    mongo_ip = ask_ip("MongoDB", "127.0.0.1")

    with open(connections_path, "r", encoding="utf-8") as f:
        connections = json.load(f)

    connections["mongo_default"]["host"] = mongo_ip

    with open(connections_path, "w", encoding="utf-8") as f:
        json.dump(connections, f, indent=2, ensure_ascii=False)

    print(f"  connections.json atualizado: mongo_default.host = {mongo_ip}")

    run(f"docker cp {connections_path} airflow-demo-test:/tmp/connections.json")
    run("docker exec airflow-demo-test airflow connections import /tmp/connections.json")


def step_import_variables(repo_dir):
    banner("3. Importacao das Airflow Variables")
    variables_path = os.path.join(repo_dir, "docs", "variables.json")
    run(f"docker cp {variables_path} airflow-demo-test:/tmp/variables.json")
    run("docker exec airflow-demo-test airflow variables import /tmp/variables.json")


def step_create_tables(repo_dir):
    banner("4. Criacao das tabelas no Hydra (PostgreSQL)")

    hydra_ip = ask_ip("Hydra (PostgreSQL)", "127.0.0.1")
    hydra_host = f"{hydra_ip}:5432"

    sql_script = os.path.join(repo_dir, "scripts", "create_tables.sql")
    if not os.path.isfile(sql_script):
        print(f"  ERRO: Script nao encontrado em {sql_script}")
        return

    step(f"Executando {sql_script} em {hydra_host}...")
    run(
        f'psql -h {hydra_ip} -p 5432 -U postgres -f "{sql_script}"',
        check=False,
    )


def step_trigger_dag(container):
    banner("5. Trigger do DAG metrics_etl")

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

    # Step 0: Preparar diretorios e .env (primeiros passos obrigatorios)
    step_prepare_dirs(repo_dir)
    step_prepare_env(repo_dir)

    # Check prerequisites
    for tool in ["docker"]:
        if not shutil.which(tool):
            print(f"  ERRO: '{tool}' nao encontrado. Instale-o e tente novamente.")
            sys.exit(1)

    r = run("docker compose version", check=False, capture=True)
    if r.returncode != 0:
        print("  ERRO: 'docker compose' nao disponivel.")
        sys.exit(1)

    container = "airflow-demo-test"

    # Step 1: Docker build & up
    step_docker_build(repo_dir)

    # Wait for Airflow
    wait_for_airflow(container, timeout=120)

    # Step 2: Import connections
    step_import_connections(repo_dir)

    # Step 3: Import variables
    step_import_variables(repo_dir)

    # Step 4: Create tables
    step_create_tables(repo_dir)

    # Step 5: Trigger
    step_trigger_dag(container)

    banner("Setup concluido!")
    print(f"  Airflow UI: http://localhost:8080")
    print(f"  Container:  {container}")
    print(f"  DAG:        metrics_etl")
    print(f"  Docs:       docs/airflow-configuracao.md\n")


if __name__ == "__main__":
    main()
