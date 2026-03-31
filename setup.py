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


def load_dotenv(filepath):
    """Load variables from a .env file into os.environ."""
    if not os.path.isfile(filepath):
        return
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            os.environ.setdefault(key, value)


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


def ask_host(label, default):
    """Ask for a host (IP address or hostname) with basic validation."""
    while True:
        host = ask(f"Host do {label}", default)
        if re.match(r"^[\w.\-]+$", host):
            return host
        print(f"    Host invalido: {host}. Tente novamente.")


def update_env_values(values, filepath=None):
    """Update or append key=value pairs in the .env file."""
    if filepath is None:
        filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.isfile(filepath):
        return
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    updated_keys = set()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in values:
                new_lines.append(f"{key}={values[key]}\n")
                updated_keys.add(key)
                continue
        new_lines.append(line)

    # Append any keys not already present
    for key, val in values.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={val}\n")

    with open(filepath, "w", encoding="utf-8") as f:
        f.writelines(new_lines)
    print("  .env atualizado com os valores da topologia.")


def ask_topology():
    """Ask the user about deployment topology and return host configuration."""
    banner("Topologia de Instalacao")
    print("  Escolha o tipo de instalacao:\n")
    print("  1) All-in-one   - Todos os componentes na mesma maquina (default)")
    print("  2) Distribuida  - udata (MongoDB + API) numa maquina remota\n")

    choice = ask("Opcao", "1")

    config = {
        "udata_host": os.environ.get("UDATA_HOST") or "host.docker.internal",
        "udata_port": int(os.environ.get("UDATA_PORT") or "7000"),
        "mongo_host": os.environ.get("MONGODB_HOST") or "host.docker.internal",
        "mongo_port": int(os.environ.get("MONGODB_PORT") or "27017"),
    }

    if choice == "2":
        print("\n  Modo distribuido: udata esta num servidor remoto.")
        mongo_host = ask_host("servidor MongoDB", config["mongo_host"])
        config["mongo_host"] = mongo_host
        udata_host = ask_host("servidor udata API", config["udata_host"])
        config["udata_host"] = udata_host
        config["udata_port"] = None  # remoto: sem porta (porta 80 default)
    else:
        mongo_ip = ask_ip("MongoDB (udata)", "10.55.37.40")
        config["mongo_host"] = mongo_ip

    # Persistir valores escolhidos no .env
    update_env_values({
        "UDATA_HOST": config["udata_host"],
        "UDATA_PORT": str(config["udata_port"]) if config["udata_port"] else "",
        "MONGODB_HOST": config["mongo_host"],
        "MONGODB_PORT": str(config["mongo_port"]),
    })

    banner("Configuracao de Rede")
    print(f"  MongoDB:        {config['mongo_host']}:{config['mongo_port']}")
    if config["udata_port"]:
        print(f"  udata API:      {config['udata_host']}:{config['udata_port']}")
    else:
        print(f"  udata API:      {config['udata_host']}")
    print(f"  Hydra/API-Tab:  host.docker.internal (local)")

    return config


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

    # step("Build da imagem Airflow...")
    # run(f"docker compose build --no-cache webserver", cwd=repo_dir)

    step("Arranque dos containers...")
    run(f"docker compose up -d", cwd=repo_dir)

    step("Estado dos containers:")
    run(f"docker compose ps", cwd=repo_dir)


def step_import_connections(repo_dir, topology, container):
    banner("2. Importacao das Airflow Connections")

    connections_path = os.path.join(repo_dir, "docs", "connections.json")

    with open(connections_path, "r", encoding="utf-8") as f:
        connections = json.load(f)

    # Update hosts based on topology
    connections["mongo_default"]["host"] = topology["mongo_host"]
    connections["mongo_default"]["port"] = topology["mongo_port"]
    connections["udata_http"]["host"] = topology["udata_host"]
    if topology["udata_port"]:
        connections["udata_http"]["port"] = topology["udata_port"]
    else:
        connections["udata_http"].pop("port", None)

    # Ensure local connections use host.docker.internal
    # (127.0.0.1 inside the container does not reach host services)
    for conn_id, conn in connections.items():
        if conn.get("host") == "127.0.0.1":
            connections[conn_id]["host"] = "host.docker.internal"
            print(f"  {conn_id}: host corrigido para host.docker.internal")

    with open(connections_path, "w", encoding="utf-8") as f:
        json.dump(connections, f, indent=2, ensure_ascii=False)

    print(f"  connections.json atualizado:")
    print(f"    mongo_default.host = {topology['mongo_host']}")
    print(f"    udata_http.host   = {topology['udata_host']}")

    run(f"docker cp {connections_path} {container}:/tmp/connections.json")

    # Delete existing connections to allow re-import with updated values
    for conn_id in connections:
        docker_exec(container, f"airflow connections delete {conn_id}")

    run(f"docker exec {container} airflow connections import /tmp/connections.json")


def step_import_variables(repo_dir, topology, container):
    banner("3. Importacao das Airflow Variables")
    variables_path = os.path.join(repo_dir, "docs", "variables.json")

    with open(variables_path, "r", encoding="utf-8") as f:
        variables = json.load(f)

    # Update variables based on topology
    if topology["udata_port"]:
        variables["UDATA_INSTANCE_URL"] = f"http://{topology['udata_host']}:{topology['udata_port']}"
    else:
        variables["UDATA_INSTANCE_URL"] = f"http://{topology['udata_host']}"
    variables.setdefault("METRICS_API_URL", "http://host.docker.internal:8006/api")

    with open(variables_path, "w", encoding="utf-8") as f:
        json.dump(variables, f, indent=4, ensure_ascii=False)

    print(f"  variables.json atualizado:")
    print(f"    UDATA_INSTANCE_URL = {variables['UDATA_INSTANCE_URL']}")

    run(f"docker cp {variables_path} {container}:/tmp/variables.json")
    run(f"docker exec {container} airflow variables import /tmp/variables.json")


def step_create_tables(repo_dir, container):
    banner("4. Criacao das tabelas no Hydra CSV (PostgreSQL porta 5434)")

    sql_script = os.path.join(repo_dir, "scripts", "create_tables.sql")
    if not os.path.isfile(sql_script):
        print(f"  ERRO: Script nao encontrado em {sql_script}")
        return

    step("Executando create_tables.sql via Airflow (hydra_postgres_csv)...")

    # Copy SQL script into the Airflow container
    run(f"docker cp {sql_script} {container}:/tmp/create_tables.sql")

    # Execute via Python inside the container using the Airflow connection
    # This avoids needing psql on the host and uses the correct connection
    r = docker_exec(
        container,
        'python3 -c "'
        "from airflow.providers.postgres.hooks.postgres import PostgresHook; "
        "hook = PostgresHook(postgres_conn_id='hydra_postgres_csv'); "
        "conn = hook.get_conn(); cur = conn.cursor(); "
        "cur.execute(open('/tmp/create_tables.sql').read()); "
        "conn.commit(); cur.close(); conn.close(); "
        "print('Tabelas e views criadas com sucesso')"
        '"',
    )
    if r.returncode == 0:
        print(f"  {r.stdout.strip()}")
    else:
        print(f"  ERRO: {r.stderr.strip()}")


def step_setup_api_tabular():
    banner("5. Setup api-tabular-pt com pm2")

    api_dir = "/opt/api-tabular-pt"
    if not os.path.isdir(api_dir):
        print(f"  AVISO: Diretoria {api_dir} nao encontrada. A saltar setup do api-tabular-pt.")
        return

    # Instalar Node.js (necessario para pm2) via dnf no Rocky Linux 9
    step("Instalando Node.js e npm...")
    r = run("sudo dnf install -y nodejs npm", check=False, capture=True)
    if r.returncode != 0:
        print(f"  ERRO ao instalar Node.js: {r.stderr.strip()}")
        return

    # Instalar pm2 globalmente
    step("Instalando pm2...")
    r = run("sudo npm install -g pm2", check=False, capture=True)
    if r.returncode != 0:
        print(f"  ERRO ao instalar pm2: {r.stderr.strip()}")
        return
    print(f"  {r.stdout.strip()}")

    # Arrancar api-tabular-pt com pm2
    step("Arrancando api-tabular-pt via pm2...")
    gunicorn_cmd = (
        "uv run gunicorn api_tabular.metrics.app:app_factory"
        " --bind 0.0.0.0:8006"
        " --worker-class aiohttp.GunicornWebWorker"
        " --workers 4"
        " --access-logfile -"
    )
    r = run(
        f'pm2 start "{gunicorn_cmd}" --name api-tabular-pt --cwd {api_dir}',
        check=False, capture=True,
    )
    if r.returncode == 0:
        print("  api-tabular-pt arrancado com sucesso via pm2.")
        run("pm2 save", check=False, capture=True)
    else:
        print(f"  ERRO ao arrancar api-tabular-pt: {r.stderr.strip()}")


def step_trigger_dag(container):
    banner("6. Trigger do DAG metrics_etl")

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

    # Load .env variables
    load_dotenv(os.path.join(repo_dir, ".env"))

    # Step 0: Preparar diretorios e .env (primeiros passos obrigatorios)
    step_prepare_dirs(repo_dir)
    step_prepare_env(repo_dir)

    # Reload .env after potential creation by 2_prepare_env.sh
    load_dotenv(os.path.join(repo_dir, ".env"))

    # Ask about deployment topology
    topology = ask_topology()

    # Check prerequisites
    for tool in ["docker"]:
        if not shutil.which(tool):
            print(f"  ERRO: '{tool}' nao encontrado. Instale-o e tente novamente.")
            sys.exit(1)

    r = run("docker compose version", check=False, capture=True)
    if r.returncode != 0:
        print("  ERRO: 'docker compose' nao disponivel.")
        sys.exit(1)

    env_type = os.environ.get("AIRFLOW_ENV_TYPE") or "demo"
    env_name = os.environ.get("AIRFLOW_ENV_NAME") or "test"
    container = f"airflow-{env_type}-{env_name}"

    # Step 1: Docker build & up
    step_docker_build(repo_dir)

    # Wait for Airflow
    wait_for_airflow(container, timeout=120)

    # Step 2: Import connections
    step_import_connections(repo_dir, topology, container)

    # Step 3: Import variables
    step_import_variables(repo_dir, topology, container)

    # Step 4: Create tables
    step_create_tables(repo_dir, container)

    # Step 5: Setup api-tabular-pt com pm2
    step_setup_api_tabular()

    # Step 6: Trigger
    step_trigger_dag(container)

    webserver_port = os.environ.get("AIRFLOW_WEBSERVER_PORT", "8080")
    banner("Setup concluido!")
    print(f"  Airflow UI: http://localhost:{webserver_port}")
    print(f"  Container:  {container}")
    print(f"  DAG:        metrics_etl")
    print(f"  Docs:       docs/airflow-configuracao.md\n")


if __name__ == "__main__":
    main()
