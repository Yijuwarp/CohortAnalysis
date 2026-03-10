# Short Summary
This guide explains the backend directory layout and how a request moves through routers, domains, queries, and DuckDB.

## Project structure
- `app/main.py`: app factory and router registration.
- `app/routers/`: endpoint adapters.
- `app/domains/`: business workflows.
- `app/queries/`: SQL-heavy helpers.
- `app/db/`: connection, schema, indexes, migrations.
- `app/models/`: request models.

## Request flow
Router receives payload → domain service executes logic → query helpers run SQL → db layer provides connection.

## Adding endpoints
1. Add request model in `app/models` if needed.
2. Add a router handler in `app/routers`.
3. Add or extend a domain service in `app/domains`.
4. Keep SQL in `app/queries` where practical.

## DB connection
Use `app.db.connection.get_connection()`, which resolves the runtime database path from `app.main.DATABASE_PATH` for test compatibility.
