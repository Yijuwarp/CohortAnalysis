# Short Summary
This document explains revenue event configuration and monetization inputs.

## Revenue event configuration
Revenue selection rows are stored in `revenue_event_selection` (include/exclude + override value).

## Event overrides
Override values replace per-event revenue as `event_count * override`.

## Modified columns
`events_normalized`/`events_scoped` keep `original_*` and `modified_*` revenue/count columns.

## Monetization queries
Monetization endpoints aggregate modified revenue by cohort day offsets.
