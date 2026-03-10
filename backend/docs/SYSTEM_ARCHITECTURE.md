# Short Summary
This document describes the layered backend architecture and ownership boundaries.

## Router → Domain → Query → Database
- Routers only parse HTTP contracts and call services.
- Domains own business rules and orchestration.
- Queries centralize SQL-heavy operations.
- DB layer owns connection/schema/index/migration primitives.

## Domain responsibilities
- Ingestion: upload, mapping, type detection, normalization.
- Scope: filters and scoped dataset metadata.
- Cohorts: definitions, membership, activity snapshots.
- Revenue: event selection and recomputation.
- Analytics: retention, usage, monetization output models.

## Data pipeline overview
`events` (raw upload) → `events_normalized` (canonical schema) → `events_scoped` (active filters) → cohort membership/activity → analytics responses.
