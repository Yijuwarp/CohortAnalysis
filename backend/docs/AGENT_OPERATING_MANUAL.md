# Short Summary
This manual tells AI agents where to add code and what architectural rules to preserve.

## System Overview
The backend is layered: routers delegate to domains, domains orchestrate queries, and DB modules manage DuckDB primitives.

## Directory Structure
- `app/routers`: HTTP contracts.
- `app/domains`: business workflows.
- `app/queries`: SQL-heavy query helpers.
- `app/db`: connection/schema/indexes/migrations.
- `app/models`: request payload schemas.
- `docs/`: architecture and subsystem references.

## Domain Responsibilities
Keep ingestion, scope, cohorts, analytics, and revenue logic in their domain folders.

## Data Pipeline
Preserve `events` → `events_normalized` → `events_scoped` and downstream cohort/analytics tables.

## Cohort Engine
Cohort CRUD and membership refresh belong in `domains/cohorts` and related query helpers.

## Analytics System
Retention/usage/monetization endpoints should remain thin router adapters calling analytics services.

## Rules for AI Agents
1. Do not change endpoint paths or response shapes without explicit request.
2. Keep SQL centralized and avoid copy/paste duplication.
3. Add module docstrings with a one-sentence summary.
4. Prefer small focused modules over large monoliths.
5. Preserve backward-compatible DB semantics.
