# Frontend Overview

The frontend is a modern React SPA built with Vite, designed for interactive and dynamic cohort analysis.

## Core UI Components & Hierarchy

- **App (`App.jsx`)**: The central entry point. It manages global state (app state, dataset metadata, active tab) and coordinates the transition between onboarding, mapping, and the workspace.
- **Top Toolbar (`components/TopToolbar`)**: Contains global controls for dataset management, revenue configuration, cohort creation, and date range filtering.
- **Sidebar (`LeftPane`)**: A collapsible panel hosting three main tabs:
    - **Filters**: Real-time data scoping using `FilterData`.
    - **Analytics Settings**: Global settings like `max_day`, `retention_event`, and the `RevenueConfig` interface.
    - **Cohorts**: The `CohortPane` for creating, editing, and managing cohorts.
- **Analytics Area**: The main workspace where different analytics tables and visualizations are rendered:
    - **RetentionTable**: Visualizes user retention metrics.
    - **UsageTable**: Displays event volume and user counts.
    - **MonetizationTable**: Shows revenue and conversion data.
    - **FunnelPane**: Interactive funnel creation and visualization.
    - **FlowPane**: SANKY-like flow diagrams for event sequences.
    - **UserExplorer**: A deep-dive interface for inspecting individual user activity timelines.

## State Management

- **Local State**: Extensive use of `useState` and `useMemo` within components for localized UI state.
- **Persistence**: Workspace state (active filters, selected events, global settings) is persisted in `localStorage` to survive page reloads.
- **API Integration (`api.js`)**: A centralized communication layer that handles all requests to the FastAPI backend.

## Key UI Systems

- **Mapping Wizard**: Guides the user through the initial CSV column mapping process.
- **Searchable Selects**: Custom components for navigating large event and property lists.
- **Filtering System**: Allows users to build complex `AND/OR` filter logic for dataset scoping.
- **Cohort Management**: A structured workflow for defining cohorts based on event frequency and property matches.

## Styling & Design

- **CSS**: Vanilla CSS with a focus on responsiveness, dark-mode-ready color palettes, and interactive elements.
- **Layout**: Flexible CSS Grid and Flexbox layouts for the workspace and pane systems.
