# Frontend Overview

The frontend is a modern React SPA built with Vite, designed for interactive and dynamic cohort analysis.

## Core UI Components & Hierarchy

- **App (`App.jsx`)**: The central entry point. It manages global state (app state, dataset metadata, active tab) and coordinates the transition between onboarding, mapping, and the workspace.
- **Top Toolbar (`components/TopToolbar`)**: Contains global controls for dataset management, revenue configuration, cohort creation, and date range filtering.
- **Sidebar (`LeftPane`)**: A collapsible panel hosting three main sections:
    - **Filters**: Real-time data scoping using `FilterData`.
    - **Analytics Settings**: Global settings like `max_day`, `retention_event`, and `RevenueConfig`.
    - **Cohorts**: The `CohortPane` for creating, editing, and managing cohorts.
- **Analytics Area**: The main workspace where different analytical lenses are presented as tabs:
    - **Retention**: Visualizes user retention metrics using `RetentionTable`.
    - **Usage**: Displays event volume and user counts using `UsageTable`.
    - **Monetization**: Tables for revenue and user value using `MonetizationTable`.
    - **Paths**: Interactive sequence analysis using `PathsPane`.
    - **Flows**: Sankey-style transition graphs using `FlowPane`.
    - **User Explorer**: Deep-dive into individual timelines using `UserExplorer`.

## State Management

- **Local State**: Extensive use of `useState` and `useMemo` for localized UI state.
- **Persistence**: Workspace state (active filters, selected events, global settings) is persisted in `localStorage` (`cohort-analysis-workspace-v2`).
- **Tab Reloading**: Tabs track their own "stale" state. When global filters change, tabs are marked as stale, and the user is prompted to reload the data to ensure accuracy.

## Key UI Systems

- **Mapping Wizard**: Guides the user through initial CSV column mapping and type detection.
- **Searchable Selects**: Custom components for efficiently navigating large event and property lists.
- **Filtering System**: Allows users to build complex `AND/OR` filter logic for dataset scoping.
- **Cohort Management**: A structured workflow for defining cohorts based on frequency and property matches.

## Styling & Design

- **CSS**: Vanilla CSS with a focus on modern aesthetic, responsive layouts, and interactive micro-animations.
- **Layout**: Flexible CSS Grid and Flexbox for the workspace and pane systems.
