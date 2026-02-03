# Dropit fetcher kt
Dropit fetcher kt is a kotlin fetcher to fetch product prices in https://www.dropit.bm/. 


## Project overview 
this project is divided into 2 parts:
First part, to fetch the price of diff products and using parser to parse info.
Second part, it save into postgresql db (if connected) or to json files for human evaluvate.



## Tech relate stuff
Dropit is use api.freshop.ncrcloud.com as backend service, which is owned by NCR Voyix Freshop.
We can find the api spec in https://developer.ncrvoyix.com/portals/dev-portal/api-explorer/details/1186/documentation
But in this project, we would only focus on the product list and department list.

## Configuration
You can provide a `.env` file to configure the temp output folder used when saving JSON responses and Postgres connection info.

Example:
```
TEMP_FOLDER=./temp
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=dropit
POSTGRES_USER=dropit_user
POSTGRES_PASSWORD=dropit_password
```

### Environment variables
- `TEMP_FOLDER`: directory where JSON snapshots are written locally.
- `POSTGRES_HOST`: Postgres hostname (required to enable database writes).
- `POSTGRES_PORT`: Postgres port.
- `POSTGRES_DB`: Postgres database name.
- `POSTGRES_USER`: Postgres username.
- `POSTGRES_PASSWORD`: Postgres password.
