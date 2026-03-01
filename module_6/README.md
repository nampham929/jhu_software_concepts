# Module 6

## Overview
This module converts the GradCafe analytics project into a containerized microservice stack.

Services:
- `web`: Flask app on port `8080`
- `worker`: RabbitMQ consumer for background tasks
- `db`: PostgreSQL 16
- `rabbitmq`: RabbitMQ management image

Background tasks:
- `scrape_new_data`
- `recompute_analytics`

The web service publishes tasks to RabbitMQ. The worker consumes them, updates PostgreSQL, and writes shared job status so the UI can show queued/running/completed state.

## Project Structure
```text
module_6/
  docker-compose.yml
  setup.py
  README.md
  docs/
  tests/
  src/
    web/
      Dockerfile
      requirements.txt
      run.py
      publisher.py
      app/
    worker/
      Dockerfile
      requirements.txt
      consumer.py
      etl/
    db/
      load_data.py
    data/
      llm_extend_applicant_data.json
```

## Ports
- Web UI: `http://localhost:8080`
- RabbitMQ UI: `http://localhost:15672`
- PostgreSQL: `localhost:5432`
- RabbitMQ AMQP: `localhost:5672`

RabbitMQ default dev login:
- username: `guest`
- password: `guest`

## Environment
The local `.env` used for Docker Compose should contain:

```env
DATABASE_URL=postgresql://app_user:app_password@db:5432/applicants
```

Inside Docker, the database host must be `db`, not `localhost`.

## Run With Docker Compose
From the `module_6` root:

```powershell
docker compose down
docker compose up --build
```

If you want a fresh database reseed from `src/data/llm_extend_applicant_data.json`:

```powershell
docker compose down -v
docker compose up --build
```

## What Happens At Startup
- `db` starts PostgreSQL
- `rabbitmq` starts the broker and management UI
- `worker` seeds the database from `src/data/llm_extend_applicant_data.json` if the `applicants` table is empty
- `web` serves the dashboard on port `8080`

## Task Flow
1. User clicks `Pull Data` or `Update Analysis`
2. `web` publishes a durable message to the `tasks` exchange
3. RabbitMQ routes the message to `tasks_q`
4. `worker` consumes from `tasks_q` with `prefetch=1`
5. `worker` updates PostgreSQL in a transaction
6. `worker` updates shared job status in PostgreSQL
7. UI polls `/pull-status` and updates the status panel/button state

## Seed Data
Seed file used by the worker:

- [`src/data/llm_extend_applicant_data.json`](c:/Users/namph/jhu_software_concepts/module_6/src/data/llm_extend_applicant_data.json)

This file is mounted read-only into the worker container as `/data/llm_extend_applicant_data.json`.

## Local Non-Docker Run
Docker Compose is the intended run path.

If you run `src/web/run.py` directly in a local venv, `DATABASE_URL` must use `localhost`, for example:

```powershell
$env:DATABASE_URL="postgresql://app_user:app_password@localhost:5432/applicants"
python src\web\run.py
```

Do not use `@db` when running directly on Windows outside Docker.

## Tests
Current test command:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Current status at last check:
- `56 passed`
- `100.00% coverage`

The test suite now targets the module_6 runtime paths (`src/web/app`, `src/db`) instead of only the legacy root `src/...` layout.

## Lint
Current lint command:

```powershell
.\.venv\Scripts\python.exe -m pylint --recursive=y --fail-under=10 src/web src/worker src/db
```

Current status at last check:
- `10.00/10`

Lint is scoped to the module_6 runtime implementation (`src/web`, `src/worker`, `src/db`).

## Build Images
Compose builds:
- `module_6-web`
- `module_6-worker`

Manual build examples:

```powershell
docker build -t <dockerhub-user>/module_6:web-v1 .\src\web
docker build -t <dockerhub-user>/module_6:worker-v1 .\src\worker
```

## Push Images To Docker Hub
Example commands:

```powershell
docker login
docker push <dockerhub-user>/module_6:web-v1
docker push <dockerhub-user>/module_6:worker-v1
```

Registry links:
- Web image: `https://hub.docker.com/r/nampham929/module_6`
- Worker image: `https://hub.docker.com/r/nampham929/module_6`
- Repository: `https://hub.docker.com/r/nampham929/module_6`

Published tags:
- `web-v1`
- `worker-v1`

## Submission Checklist
- `docker compose up --build` runs all four services
- Web dashboard loads on `localhost:8080`
- RabbitMQ UI loads on `localhost:15672`
- `Pull Data` queues and is consumed
- `Update Analysis` queues and is consumed
- Screenshots captured for web and RabbitMQ
- Docker Hub images pushed and links added
- README updated with final registry links
- CI paths updated to module_6
- Final pylint and pytest pass confirmed
