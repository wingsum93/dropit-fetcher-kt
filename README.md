# Dropit Fetcher KT

Dropit Fetcher KT is a Kotlin service that scrapes product and department data from Dropit/Freshop APIs and stores it in Postgres.

## Modules

- `scraper-core`: shared scraper logic, Freshop HTTP client, parser/mappers, queue-aware `ProductScraper`, Postgres storage, and distributed rate limiting.
- `server`: Ktor API server. It creates scrape jobs and exposes status/read endpoints.
- `scraper-worker`: long-running worker process. Multiple replicas poll Postgres and execute scraper jobs.

## Configuration

Create a `.env` file:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=dropit
POSTGRES_USER=dropit_user
POSTGRES_PASSWORD=dropit_password
SERVER_PORT=8080
WORKER_POLL_INTERVAL_MS=2000
WORKER_BATCH_SIZE=1
```

Postgres configuration is required. The app no longer falls back to SQLite.

## API

- `GET /health`
- `POST /scrapes`
- `GET /scrapes/{syncId}`
- `GET /scrapes/{syncId}/jobs?status=&type=`
- `GET /products?limit=&since=`
- `GET /departments`

Start a scrape:

```sh
curl -X POST http://localhost:8080/scrapes
```

The server returns `202 Accepted` with a sync id. Worker replicas pick up the scrape jobs from Postgres.

## Local Build

```sh
./gradlew test
./gradlew build
```

Run the server:

```sh
./gradlew :server:run
```

Run a worker:

```sh
./gradlew :scraper-worker:run
```

## Docker Compose

Start Postgres, the API server, and three worker replicas:

```sh
docker compose up --build --scale scraper-worker=3
```

The server is available at `http://localhost:${SERVER_PORT:-8080}`.
