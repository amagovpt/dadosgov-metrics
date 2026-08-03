# Backup das métricas do Matomo: MariaDB → ficheiros → MongoDB

Substitui a extracção via Reporting API HTTP do
[`import_matomo_metrics.py`](import_matomo_metrics.py), que deixou de funcionar
porque `https://dados.gov.pt/stats/` já não está disponível na intranet.

São dois scripts, um por VM:

| Script | Onde corre | O que faz |
|---|---|---|
| [`export_matomo_mariadb.py`](export_matomo_mariadb.py) | **VM do Matomo** | Lê a MariaDB (read-only) e escreve NDJSON comprimido + manifesto |
| [`import_matomo_export_to_mongo.py`](import_matomo_export_to_mongo.py) | **VM do MongoDB** | Valida os ficheiros e carrega-os em colecções; resolve `slug → ObjectId` |

O script antigo continua no repositório: é a referência do mapeamento
URL→objecto e do caminho para o PostgreSQL `metric.*`, que não foi alterado.

---

## Passo 0 — Instalar o driver na VM do Matomo

```bash
pip install pymysql          # puro-Python, não precisa de headers de sistema
```

O script também aceita `mysqlclient` ou `mysql-connector-python` se já estiverem
instalados. Não precisa do cliente `mysql`/`mysqldump` — liga-se por TCP, o que
funciona tanto com MariaDB nativa como em container com porta publicada.

## Passo 1 — Diagnóstico (correr **sempre** primeiro)

```bash
python3 export_matomo_mariadb.py --info \
    --config-ini /var/www/matomo/config/config.ini.php
```

Mostra, e escreve em `coverage.json`:

- os sites e respectivos `idsite` — **confirmar qual é o de produção** (o script
  antigo assumia `idSite=3`);
- o histórico **realmente existente** em `matomo_log_link_visit_action`: data
  mínima/máxima e nº de acções por mês;
- se a **purga de dados em bruto** está activa. Se estiver, o histórico anterior
  ao limite de retenção só existe nos arquivos agregados `matomo_archive_*`, que
  estes scripts não lêem — nesse caso o export cobre só o que resta nos logs;
- as versões da MariaDB e do Matomo.

Se preferir variáveis de ambiente às credenciais do `config.ini.php`, copie
[`.env.matomo-export.example`](.env.matomo-export.example) e ajuste.

## Passo 2 — Export de um dia (validar antes do histórico todo)

```bash
python3 export_matomo_mariadb.py --site-id 3 \
    --date-from 2026-07-30 --date-to 2026-07-30 \
    --config-ini /var/www/matomo/config/config.ini.php
```

Conferir no ecrã que `visits_daily` tem documentos e que os slugs fazem sentido:

```bash
zcat matomo_export_3_2026-07-30_2026-07-30/agg/visits_daily.ndjson.gz | head
```

## Passo 3 — Export completo

```bash
python3 export_matomo_mariadb.py --site-id 3 --all \
    --verify-archive --tar \
    --config-ini /var/www/matomo/config/config.ini.php
```

`--all` vai da primeira acção registada até ontem. Processa mês a mês, pelo que
pode ser interrompido e retomado: os meses já escritos são mantidos (use
`--overwrite` para os refazer).

## Passo 4 — Copiar e importar

```bash
scp matomo_export_3_*.tar.gz <vm-mongo>:/tmp/

# na VM do MongoDB
pip install pymongo
python3 import_matomo_export_to_mongo.py /tmp/matomo_export_3_*.tar.gz --dry-run
python3 import_matomo_export_to_mongo.py /tmp/matomo_export_3_*.tar.gz --resolve-udata
```

O `--dry-run` lê tudo, valida os checksums e conta, sem escrever nada. Um
ficheiro corrompido ou em falta aborta o import com código de saída ≠ 0.

Reimportar o mesmo export é inofensivo: cada documento traz um `_id`
determinista, calculado no exportador, logo a segunda passagem substitui em vez
de duplicar.

---

## O que é extraído

| Família | `log_action.type` | Coluna de ligação | Métrica |
|---|---|---|---|
| pageviews | 1 | `idaction_url` | `nb_visits` (= `COUNT(DISTINCT idvisit)`), `nb_hits` |
| outlinks | 2 | `idaction_url` | idem, atribuído pela página de origem |
| downloads | 3 | `idaction_url` | idem, atribuído pela página de origem |
| site search | 8 | `idaction_name` | `nb_searches`, `nb_visits`, `nb_results` |
| eventos | 10 / 11 / 12 | `idaction_event_category` / `idaction_event_action` / `idaction_name` | `nb_visits`, `nb_hits` |

O mapeamento URL→objecto usa os mesmos regex do script antigo
([`import_matomo_metrics.py:57-63`](import_matomo_metrics.py#L57-L63)), para os
números serem comparáveis: `datasets`, `reuses`, `organizations`,
`dataservices`, `resources`, todos sob um prefixo de língua `/pt|/en|/fr|/es`.

**Outlinks e downloads** são atribuídos ao objecto udata pela **página de
origem** (`idaction_url_ref`), não pelo destino do clique — o destino é externo.
É o mesmo critério do pipeline datagouv
(`segment=actionUrl==.../{model}/{slug}/`). Cada documento leva os dois:
`target_url` e `object_type`/`object_ref` da origem.

### Duas correcções face ao script antigo

1. **Fuso horário.** `server_time` está em UTC, mas a API do Matomo agrupa os
   dias no fuso do site (`Europe/Lisbon`). O exportador converte com `zoneinfo`;
   o manifesto regista em `day_bucketing` se conseguiu (`site-timezone`) ou se
   caiu para UTC. `--utc-days` força UTC.
2. **`nb_visits` é `COUNT(DISTINCT idvisit)`**, não `COUNT(*)`. `COUNT(*)` é
   `nb_hits`. Ambos são exportados.

---

## Estrutura do export

```
matomo_export_3_2018-07-19_2026-07-30/
  manifest.json      # site, fuso, intervalo, contagens, sha256 de cada ficheiro,
                     # versões, opções usadas, resultado da validação
  coverage.json      # o mesmo relatório do --info
  checksums.sha256
  site.ndjson.gz
  agg/  visits_daily | outlinks_daily | downloads_daily | searches_daily | events_daily
  raw/  pageviews/YYYY-MM | outlinks/… | downloads/… | searches/… | events/…
```

Os ficheiros `agg/` são o equivalente ao que a API devolvia. Os `raw/` são as
linhas de acção que os originaram (`_id` = `idlink_va`), para permitir recalcular
ou auditar na outra VM sem voltar à VM do Matomo. `--no-raw` salta-os;
`--raw-all-actions` exporta todas as acções, não só as que casam com os padrões.

### Colecções criadas no MongoDB

Base de dados `matomo` (configurável com `--mongo-db`):

`visits_daily`, `outlinks_daily`, `downloads_daily`, `searches_daily`,
`events_daily`, `actions_raw` (todas as famílias em bruto, distinguidas por
`action_type`), `site`, `imports` (registo de cada execução), `unresolved`.

## Validação disponível

Sem a API web, a única referência independente são as tabelas de arquivo do
Matomo. `--verify-archive` compara, por dia, o `COUNT(DISTINCT idvisit)` agregado
com `matomo_archive_numeric_*` (`name='nb_visits'`, `period=1`) e escreve o
resultado em `manifest.json`:

- **`logs <= arquivo` é o normal** — o arquivo conta todas as visitas do site, o
  export só as que tocaram páginas de objectos udata;
- **`logs > arquivo` (`suspect_days`) não deveria acontecer** — é o sinal de
  alarme. `verdict` é `ok` ou `SUSPEITO`.

Slugs que não existam no udata aparecem na colecção `unresolved` com a contagem
de documentos afectados, em vez de desaparecerem sem rasto (o script antigo
fazia `continue` silencioso).

## Limites deliberados

- **Não lê `matomo_archive_blob_*`.** Se o `--info` revelar purga dos logs em
  bruto, o histórico anterior ao limite de retenção não é recuperável por aqui —
  exigiria descomprimir e interpretar o formato interno dos blobs do Matomo.
- **Não escreve nas colecções do udata** (`dataset.metrics.*`, `metrics`,
  `site`). Essas pertencem ao DAG [`metrics_etl`](../dags/metrics_etl.py), que as
  reescreve a cada 15 minutos; escrever daqui criaria um terceiro autor no mesmo
  campo. O staging fica na base de dados `matomo`, para ser consumido depois.
- **Não liga o fluxo novo ao PostgreSQL `metric.*` nem ao Airflow.**

## Nota de segurança

O `MATOMO_TOKEN` em [`import_matomo_metrics.py:38`](import_matomo_metrics.py#L38)
está em texto simples e commitado no histórico de git. Continua válido para a
API do Matomo e convém ser rodado. Os scripts novos nunca têm credenciais no
código: lêem do `config.ini.php`, do ambiente ou dos argumentos.
