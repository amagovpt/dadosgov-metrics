-- 2026-10-15: __id de metric.visits_datasets e metric.visits_resources passa a bigint.
--
-- O create_tables.sql continua a criar as tabelas com SERIAL (integer): esta
-- migracao corre tanto em BDs existentes como a seguir a uma instalacao nova.
-- Idempotente: numa coluna ja bigint os ALTER nao fazem nada.
--
-- Porque: o send_to_metrics_db escrevia com INSERT ... ON CONFLICT DO UPDATE e o
-- Postgres avalia o DEFAULT nextval() antes de detetar o conflito, por isso cada
-- linha re-enviada (96 runs/dia) gastava um valor da sequencia mesmo acabando em
-- UPDATE. O visits_datasets___id_seq chegou a 2147483647 em 2026-09-12 e o ETL
-- parou. O DAG ja nao consome a sequencia nos updates; o bigint desbloqueia a
-- sequencia esgotada e fica como margem.
--
-- As materialized views leem __id (metrics_datasets, resources, resources_total)
-- e impedem o ALTER COLUMN TYPE, por isso sao largadas aqui e recriadas pelo
-- create_tables.sql. Aplicar com o scripts/apply_schema.py, que corre os dois numa
-- so transacao (quem le, a metrics-api, nunca ve as views em falta) e regista a
-- migracao em metric.schema_migrations:
--
--   docker exec <container-airflow> python3 /opt/airflow/scripts/apply_schema.py
--
-- Sem o script, o equivalente manual e:
--
--   psql -v ON_ERROR_STOP=1 -1 -f scripts/migrations/001_20261015_visits_id_bigint.sql \
--                              -f scripts/create_tables.sql
--
-- Reescreve as duas tabelas (~1M e ~0,3M linhas) com lock exclusivo: segundos.

-- Mesma ordem do create_tables.sql: quem depende cai primeiro.
DROP MATERIALIZED VIEW IF EXISTS metric.dataservices_total;
DROP MATERIALIZED VIEW IF EXISTS metric.resources_total;
DROP MATERIALIZED VIEW IF EXISTS metric.organizations_total;
DROP MATERIALIZED VIEW IF EXISTS metric.reuses_total;
DROP MATERIALIZED VIEW IF EXISTS metric.datasets_total;
DROP MATERIALIZED VIEW IF EXISTS metric.site;
DROP MATERIALIZED VIEW IF EXISTS metric.dataservices;
DROP MATERIALIZED VIEW IF EXISTS metric.resources;
DROP MATERIALIZED VIEW IF EXISTS metric.organizations;
DROP MATERIALIZED VIEW IF EXISTS metric.reuses;
DROP MATERIALIZED VIEW IF EXISTS metric.datasets;
DROP MATERIALIZED VIEW IF EXISTS metric.metrics_organizations;
DROP MATERIALIZED VIEW IF EXISTS metric.metrics_dataservices;
DROP MATERIALIZED VIEW IF EXISTS metric.metrics_reuses;
DROP MATERIALIZED VIEW IF EXISTS metric.metrics_datasets;

ALTER TABLE metric.visits_datasets ALTER COLUMN __id TYPE BIGINT;
ALTER SEQUENCE metric.visits_datasets___id_seq AS BIGINT;
ALTER TABLE metric.visits_resources ALTER COLUMN __id TYPE BIGINT;
ALTER SEQUENCE metric.visits_resources___id_seq AS BIGINT;
