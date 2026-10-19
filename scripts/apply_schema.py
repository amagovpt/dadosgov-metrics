#!/usr/bin/env python3
"""
Aplica o schema `metric` no Hydra CSV: create_tables.sql + migracoes pendentes.

Corre DENTRO do container do Airflow (usa a connection hydra_postgres_csv):

    docker exec <container-airflow> python3 /opt/airflow/scripts/apply_schema.py
    docker exec <container-airflow> python3 /opt/airflow/scripts/apply_schema.py --dry-run

O setup.py chama-o no passo 4. Serve tambem para o deploy de uma migracao num
ambiente ja instalado: pausar o metrics_etl, git pull, correr isto, despausar.

Tudo numa unica transaccao, por esta ordem:
  1. create_tables.sql -- cria o que faltar (instalacao nova) e recria as
     materialized views;
  2. cada ficheiro de scripts/migrations/ ainda nao registado em
     metric.schema_migrations, por ordem do nome (NNN_AAAAMMDD_descricao.sql);
  3. se correu alguma migracao, create_tables.sql outra vez: as migracoes podem
     largar as views (ex.: para um ALTER COLUMN TYPE) e e o create_tables que
     as volta a criar.
Se algo falhar, ROLLBACK e a base de dados fica exatamente como estava. Quem le
(metrics-api) continua a ver as views antigas ate ao commit.

Uma migracao aplicada a mao (psql) antes deste script existir nao esta
registada e volta a correr uma vez: por isso as migracoes devem ser
idempotentes (a 001 e).
"""

import argparse
import glob
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CREATE_TABLES = os.path.join(HERE, "create_tables.sql")
MIGRATIONS_DIR = os.path.join(HERE, "migrations")


def pending_migrations(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS metric.schema_migrations (
            name CHARACTER VARYING PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    cursor.execute("SELECT name FROM metric.schema_migrations")
    applied = {row[0] for row in cursor.fetchall()}
    files = sorted(glob.glob(os.path.join(MIGRATIONS_DIR, "*.sql")))
    return [f for f in files if os.path.basename(f) not in applied]


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="aplica tudo e faz ROLLBACK no fim: mostra o que correria",
    )
    args = parser.parse_args()

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    conn = PostgresHook(postgres_conn_id="hydra_postgres_csv").get_conn()
    cursor = conn.cursor()
    try:
        with open(CREATE_TABLES, encoding="utf-8") as f:
            create_tables = f.read()
        # O schema `metric` tem de existir antes da tabela de registo.
        cursor.execute(create_tables)

        todo = pending_migrations(cursor)
        for path in todo:
            name = os.path.basename(path)
            print(f"  migracao: {name}")
            with open(path, encoding="utf-8") as f:
                cursor.execute(f.read())
            cursor.execute(
                "INSERT INTO metric.schema_migrations (name) VALUES (%s)", (name,)
            )
        if todo:
            cursor.execute(create_tables)
        else:
            print("  sem migracoes pendentes")
    except Exception as exc:
        conn.rollback()
        print(f"  ERRO (rollback, nada foi alterado): {exc}", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        conn.rollback()
        print("  --dry-run: ROLLBACK, nada foi alterado")
    else:
        conn.commit()
        print("  Schema aplicado com sucesso")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
