/**
 * Frontend Vitest + React Testing Library tests for ComparePane.
 * All seven required test cases are covered.
 *
 * NOTE: tests/ is a sibling of src/, so relative paths are ../src/...
 */
import { describe, test, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

// Mock api module – path relative to test file location (tests/ → ../src/api)
vi.mock('../src/api', () => ({
  listCohorts: vi.fn(),
  compareCohorts: vi.fn(),
}))

import ComparePane from '../src/components/ComparePane'
import { listCohorts, compareCohorts } from '../src/api'

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MOCK_COHORTS = [
  { cohort_id: 1, cohort_name: 'All Users', is_active: true, hidden: false },
  { cohort_id: 2, cohort_name: 'Power Users', is_active: true, hidden: false },
  { cohort_id: 3, cohort_name: 'Hidden Cohort', is_active: true, hidden: true },
]

const MOCK_RESULT = {
  metric_label: 'Day 7 Retention Rate',
  cohort_a_value: 0.8,
  cohort_b_value: 0.5,
  difference: 0.3,
  relative_lift: 0.6,
  p_value: 0.02,
  significant: true,
  tests: [
    { name: 'two_proportion_z_test', p_value: 0.02 },
    { name: 'fisher_exact', p_value: 0.025 },
  ],
}

// p_value 0.02 → formatPValue yields "0.020" (3 dp)
const EXPECTED_P_VALUE_TEXT = '0.020'

beforeEach(() => {
  vi.clearAllMocks()
  listCohorts.mockResolvedValue({ cohorts: MOCK_COHORTS })
  compareCohorts.mockResolvedValue(MOCK_RESULT)
})

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

function renderPane(props = {}) {
  return render(
    <ComparePane
      isOpen={true}
      onClose={vi.fn()}
      tab="retention"
      maxDay={7}
      defaultMetric="retention_rate"
      retentionEvent={null}
      {...props}
    />
  )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('ComparePane', () => {
  test('compare_pane_opens_when_isOpen_true', async () => {
    renderPane({ isOpen: true })
    const pane = screen.getByTestId('compare-pane')
    expect(pane).toBeInTheDocument()
    expect(pane.className).toContain('open')
  })

  test('compare_pane_closed_when_isOpen_false', () => {
    renderPane({ isOpen: false })
    const pane = screen.getByTestId('compare-pane')
    expect(pane.className).not.toContain('open')
  })

  test('cohort_dropdown_filters_selected_cohort – selecting A removes it from B options', async () => {
    renderPane()

    // Wait for cohorts to load
    await waitFor(() => {
      expect(screen.getByTestId('compare-cohort-a')).toBeInTheDocument()
    })

    // Select "All Users" as cohort A
    fireEvent.change(screen.getByTestId('compare-cohort-a'), { target: { value: '1' } })

    // Cohort B should not contain "All Users" (cohort_id=1)
    const cohortBSelect = screen.getByTestId('compare-cohort-b')
    const options = Array.from(cohortBSelect.options).map(o => o.value)
    expect(options).not.toContain('1')
    // Cohort B should contain "Power Users" (cohort_id=2)
    expect(options).toContain('2')
  })

  test('run_button_enabled_when_both_cohorts_auto_selected', async () => {
    renderPane()

    await waitFor(() => {
      expect(screen.getByTestId('compare-cohort-a')).toBeInTheDocument()
      expect(screen.getByTestId('compare-cohort-b')).toBeInTheDocument()
    })

    // With two visible cohorts, both dropdowns should auto-populate and
    // the run button should be enabled.
    expect(screen.getByTestId('compare-cohort-a')).toHaveValue('1')
    expect(screen.getByTestId('compare-cohort-b')).toHaveValue('2')
    expect(screen.getByTestId('compare-run-button')).not.toBeDisabled()
  })

  test('compare_api_called_with_correct_payload – API called with right arguments', async () => {
    renderPane()

    await waitFor(() => {
      expect(screen.getByTestId('compare-cohort-a')).toBeInTheDocument()
    })

    fireEvent.change(screen.getByTestId('compare-cohort-a'), { target: { value: '1' } })
    fireEvent.change(screen.getByTestId('compare-cohort-b'), { target: { value: '2' } })

    fireEvent.click(screen.getByTestId('compare-run-button'))

    await waitFor(() => {
      expect(compareCohorts).toHaveBeenCalledWith(
        expect.objectContaining({
          cohort_a: 1,
          cohort_b: 2,
          tab: 'retention',
          metric: 'retention_rate',
          day: 7,
          event: null,
        })
      )
    })
  })

  test('compare_api_payload_includes_granularity_and_retention_type', async () => {
    renderPane()

    await waitFor(() => {
      expect(screen.getByTestId('compare-cohort-a')).toBeInTheDocument()
    })

    fireEvent.change(screen.getByTestId('compare-cohort-a'), { target: { value: '1' } })
    fireEvent.change(screen.getByTestId('compare-cohort-b'), { target: { value: '2' } })

    fireEvent.click(screen.getByTestId('compare-run-button'))

    await waitFor(() => {
      expect(compareCohorts).toHaveBeenCalledWith(
        expect.objectContaining({
          granularity: 'day',
          retention_type: 'classic',
        })
      )
    })
  })

  test('results_render_correctly – result data appears after running', async () => {
    renderPane()

    await waitFor(() => {
      expect(screen.getByTestId('compare-cohort-a')).toBeInTheDocument()
    })

    fireEvent.change(screen.getByTestId('compare-cohort-a'), { target: { value: '1' } })
    fireEvent.change(screen.getByTestId('compare-cohort-b'), { target: { value: '2' } })
    fireEvent.click(screen.getByTestId('compare-run-button'))

    await waitFor(() => {
      expect(screen.getByTestId('compare-results')).toBeInTheDocument()
    })

    // Should show p-value (dynamic formatter, 4 dp = '0.0200')
    expect(screen.getByTestId('compare-p-value')).toHaveTextContent(EXPECTED_P_VALUE_TEXT)
    // Should show significance badge
    expect(screen.getByTestId('compare-significant')).toBeInTheDocument()
  })

  test('pane_closes_on_x – clicking X button calls onClose', async () => {
    const onClose = vi.fn()
    render(
      <ComparePane
        isOpen={true}
        onClose={onClose}
        tab="retention"
        maxDay={7}
        defaultMetric="retention_rate"
      />
    )

    const closeBtn = screen.getByTestId('compare-pane-close')
    fireEvent.click(closeBtn)
    expect(onClose).toHaveBeenCalled()
  })

  test('pane_closes_on_escape – pressing ESC key calls onClose', async () => {
    const onClose = vi.fn()
    render(
      <ComparePane
        isOpen={true}
        onClose={onClose}
        tab="retention"
        maxDay={7}
        defaultMetric="retention_rate"
      />
    )

    fireEvent.keyDown(window, { key: 'Escape', bubbles: true })
    expect(onClose).toHaveBeenCalled()
  })

  test('swap_button_swaps_variant_and_baseline', async () => {
    renderPane()

    await waitFor(() => {
      expect(screen.getByTestId('compare-cohort-a')).toBeInTheDocument()
      expect(screen.getByTestId('compare-cohort-b')).toBeInTheDocument()
    })

    // After auto-select: Variant=1, Baseline=2
    expect(screen.getByTestId('compare-cohort-a')).toHaveValue('1')
    expect(screen.getByTestId('compare-cohort-b')).toHaveValue('2')

    // Click swap
    fireEvent.click(screen.getByTestId('compare-swap-button'))

    // Now Variant=2, Baseline=1
    expect(screen.getByTestId('compare-cohort-a')).toHaveValue('2')
    expect(screen.getByTestId('compare-cohort-b')).toHaveValue('1')

    // Run button must NOT have been clicked automatically
    expect(compareCohorts).not.toHaveBeenCalled()
  })
})
