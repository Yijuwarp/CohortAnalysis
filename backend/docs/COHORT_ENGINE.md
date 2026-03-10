# Short Summary
This document describes cohort definitions, membership building, and activity snapshots.

## Cohort definitions
Cohorts are stored in `cohorts` and `cohort_conditions` with AND/OR condition logic and join type behavior.

## Membership computation
Membership is rebuilt into `cohort_membership` by evaluating condition CTEs and join-time semantics.

## Activity snapshots
`cohort_activity_snapshot` stores events for users who belong to each cohort.

## Condition logic
Each condition supports event thresholds and optional property filters with typed operator validation.
