# Short Summary
This document explains how CSV input becomes normalized and scoped event data.

## CSV upload
`/upload` stores CSV rows into `events` and returns detected types plus mapping suggestions.

## Column mapping
`/map-columns` validates selected columns and materializes `events_normalized` with canonical fields.

## Scoped dataset
`/apply-filters` rebuilds `events_scoped` from `events_normalized` and persists scope metadata.

## Tables
- `events`: raw upload table.
- `events_normalized`: canonical event schema.
- `events_scoped`: filtered view persisted as a table.
