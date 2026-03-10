# test-project

Monorepo with:
- `table-app`: Angular frontend
- `api`: Node.js (Express) API for endpoints

## Run locally

Install dependencies from repository root:

```bash
npm install
```

Start API (port `3001`):

```bash
npm run dev:api
```

In another terminal, start frontend (port `4200`):

```bash
npm run dev:web
```

The frontend calls `/api/hello`, proxied to the API project during development.

## Run locally (production mode)

Build Angular and serve everything from the API process:

```bash
npm run build
npm start
```

Open `http://localhost:3001`.

## Deploy on Databricks

This repository is set up so one Node.js process serves both:
- API routes (`/api/*`)
- Angular static files from `table-app/dist/table-app/browser`

Recommended commands:

- Build command:

```bash
npm install
npm run build
```

- Start command:

```bash
npm start
```

Environment variables:
- `PORT`: provided by Databricks at runtime (the server reads `process.env.PORT`)
- Databricks SQL credentials (only if needed for DB access), for example:
  - `DATABRICKS_SERVER_HOSTNAME`
  - `DATABRICKS_HTTP_PATH`
  - `DATABRICKS_TOKEN`

Health check endpoint:

```text
/health
```