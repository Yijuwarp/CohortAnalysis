import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import FlowTable from './FlowTable'

describe('FlowTable', () => {
  const cohorts = ['c1', 'c2']
  const cohortMap = {
    c1: { name: 'Cohort 1' },
    c2: { name: 'Cohort 2' }
  }

  const rootRows = [
    {
      path: ['A', '__OTHER__'],
      values: {
        c1: { user_count: 0, parent_users: 100 },
        c2: { user_count: 50, parent_users: 100 }
      },
      meta: { total_event_types: 10 }
    }
  ]

  it('enables "Show more" button if any cohort has data, even if the first one is empty', () => {
    render(
      <FlowTable
        rootRows={rootRows}
        cohorts={cohorts}
        cohortMap={cohortMap}
        expandedNodes={new Set()}
        loadingNodes={{}}
        getChildren={() => []}
        onToggle={() => {}}
        onExpandOther={() => {}}
        nodeExpansion={{}}
        maxDepth={5}
      />
    )

    const button = screen.getByText(/Show more/)
    expect(button).not.toBeDisabled()
  })

  it('disables "Show more" button if all cohorts have zero data', () => {
    const emptyRows = [
      {
        path: ['A', '__OTHER__'],
        values: {
          c1: { user_count: 0, parent_users: 100 },
          c2: { user_count: 0, parent_users: 100 }
        },
        meta: { total_event_types: 10 }
      }
    ]

    render(
      <FlowTable
        rootRows={emptyRows}
        cohorts={cohorts}
        cohortMap={cohortMap}
        expandedNodes={new Set()}
        loadingNodes={{}}
        getChildren={() => []}
        onToggle={() => {}}
        onExpandOther={() => {}}
        nodeExpansion={{}}
        maxDepth={5}
      />
    )

    const button = screen.getByText(/Show more/)
    expect(button).toBeDisabled()
  })
})
