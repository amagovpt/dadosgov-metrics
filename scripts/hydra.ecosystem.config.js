// Ecosystem pm2 dedicado aos servicos Hydra (cwd: /opt/hydra-pt).
//
// api-tabular e metrics-api correm agora em containers docker e, por isso,
// NAO fazem parte deste ecosystem. Ver dadosgov-metrics/setup.py
// (step_setup_hydra), que faz 'pm2 startOrReload' deste ficheiro.
module.exports = {
  apps: [
    {
      name: "hydra-app",
      script: "uv",
      args: [
        "run", "gunicorn", "udata_hydra.app:app_factory",
        "--bind", "0.0.0.0:8000",
        "--worker-class", "aiohttp.GunicornWebWorker",
        "--workers", "4",
        "--access-logfile", "-"
      ],
      cwd: "/opt/hydra-pt",
      interpreter: "none"
    },
    {
      name: "hydra-crawler",
      script: "uv",
      args: ["run", "udata-hydra-crawl"],
      cwd: "/opt/hydra-pt",
      interpreter: "none"
    },
    {
      name: "hydra-worker",
      script: "uv",
      args: ["run", "rq", "worker", "-c", "udata_hydra.worker"],
      cwd: "/opt/hydra-pt",
      interpreter: "none"
    }
  ]
}
