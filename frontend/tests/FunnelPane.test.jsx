/**
 * Frontend Vitest + React Testing Library tests for FunnelPane.
 * Covers: rendering, events dropdown, funnel selector ordering (valid/invalid),
 * greyed-out behavior, run button state, invalid funnel UX, and empty states.
 */
import { describe, test, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'

// Mock api module
vi.mock('../src/api', () => ({
  listFunnels: vi.fn(),
  createFunnel: vi.fn(),
  deleteFunnel: vi.fn(),
  runFunnel: vi.fn(),
  getEventProperties: vi.fn(),
  getEventPropertyValues: vi.fn(),
}))

import FunnelPane from '../src/components/FunnelPane'
import { listFunnels, runFunnel, createFunnel, deleteFunnel } from '../src/api'

// ---------------------------------------------------------------------------
// Test data
// ---------------------------------------------------------------------------

// events from App.jsx are plain strings from listEvents()
const MOCK_EVENTS_STR = ['signup', 'search', 'purchase']

const MOCK_FUNNELS_MIXED = [
  { id: 1, name: 'Valid Funnel A', is_valid: true, created_at: null, steps: [] },
  { id: 2, name: 'Invalid Funnel B', is_valid: false, created_at: null, steps: [] },
  { id: 3, name: 'Valid Funnel C', is_valid: true, created_at: null, steps: [] },
  { id: 4, name: 'Invalid Funnel D', is_valid: false, created_at: null, steps: [] },
]

const MOCK_FUNNELS_VALID_ONLY = [
  { id: 1, name: 'Signup to Purchase', is_valid: true, created_at: null, steps: [
    { event_name: 'signup', filters: [] },
    { event_name: 'purchase', filters: [{ property_key: 'category', property_value: 'electronics' }] }
  ] },
]

const MOCK_FUNNELS_INVALID_ONLY = [
  { id: 1, name: 'Bad Funnel', is_valid: false, created_at: null, steps: [] },
]

const MOCK_RUN_RESULT = {
  funnel_id: 1,
  funnel_name: 'Signup to Purchase',
  steps: ['signup', 'purchase'],
  results: [
    {
      cohort_id: 1,
      cohort_name: 'All Users',
      steps: [
        { step: 0, event_name: 'signup', users: 100, conversion_pct: 100.0, dropoff_pct: 0.0 },
        { step: 1, event_name: 'purchase', users: 42, conversion_pct: 42.0, dropoff_pct: 58.0 },
      ],
    },
  ],
}

const MOCK_RUN_RESULT_ZERO_USERS = {
  funnel_id: 1,
  funnel_name: 'Empty Funnel',
  steps: ['signup', 'purchase'],
  results: [
    {
      cohort_id: 1,
      cohort_name: 'All Users',
      steps: [
        { step: 0, event_name: 'signup', users: 0, conversion_pct: 0.0, dropoff_pct: 0.0 },
        { step: 1, event_name: 'purchase', users: 0, conversion_pct: 0.0, dropoff_pct: 0.0 },
      ],
    },
  ],
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

function renderFunnelPane(events = MOCK_EVENTS_STR) {
  return render(<FunnelPane refreshToken={0} events={events} state={{}} setState={() => {}} />)
}

beforeEach(() => {
  vi.clearAllMocks()
  listFunnels.mockResolvedValue({ funnels: [] })
  runFunnel.mockResolvedValue(MOCK_RUN_RESULT)
  createFunnel.mockResolvedValue({ id: 99, name: 'New Funnel' })
  deleteFunnel.mockResolvedValue({ deleted: true, id: 1 })
})

// ---------------------------------------------------------------------------
// 1. Rendering
// ---------------------------------------------------------------------------

describe('FunnelPane – rendering', () => {
  test('funnel_pane_renders_with_new_funnel_button', async () => {
    renderFunnelPane()
    await waitFor(() => {
      expect(screen.getByTestId('funnel-pane')).toBeInTheDocument()
    })
    expect(screen.getByTestId('funnel-new-button')).toBeInTheDocument()
  })

  test('funnel_empty_state_shown_when_no_funnels', async () => {
    listFunnels.mockResolvedValue({ funnels: [] })
    renderFunnelPane()
    await waitFor(() => {
      expect(screen.getByTestId('funnel-empty-state')).toBeInTheDocument()
    })
  })

  test('funnel_selector_present_after_funnels_load', async () => {
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()
    await waitFor(() => {
      expect(screen.getByTestId('funnel-select')).toBeInTheDocument()
    })
  })
})

// ---------------------------------------------------------------------------
// 2. Events dropdown (Issue #1 fix)
// ---------------------------------------------------------------------------

describe('FunnelPane – events dropdown', () => {
  test('builder_dropdown_shows_events_from_string_array', async () => {
    renderFunnelPane(MOCK_EVENTS_STR)
    await waitFor(() => expect(screen.getByTestId('funnel-new-button')).toBeInTheDocument())

    fireEvent.click(screen.getByTestId('funnel-new-button'))

    await waitFor(() => {
      // Step 0's event selector should contain the string events
      const select = screen.getByTestId('funnel-step-event-0')
      const options = Array.from(select.querySelectorAll('option'))
      const labels = options.map(o => o.textContent)
      expect(labels).toContain('signup')
      expect(labels).toContain('search')
      expect(labels).toContain('purchase')
    })
  })

  test('builder_dropdown_shows_no_events_message_when_empty', async () => {
    renderFunnelPane([])  // empty events
    await waitFor(() => expect(screen.getByTestId('funnel-new-button')).toBeInTheDocument())

    fireEvent.click(screen.getByTestId('funnel-new-button'))

    await waitFor(() => {
      const select = screen.getByTestId('funnel-step-event-0')
      expect(select.innerHTML).toContain('No events available in dataset')
    })
  })

  test('builder_dropdown_handles_undefined_events_gracefully', async () => {
    // events prop is undefined — should not crash
    render(<FunnelPane refreshToken={0} events={undefined} state={{}} setState={() => {}} />)
    await waitFor(() => expect(screen.getByTestId('funnel-pane')).toBeInTheDocument())

    fireEvent.click(screen.getByTestId('funnel-new-button'))
    // Should render without crashing
    expect(screen.getByTestId('funnel-builder-modal')).toBeInTheDocument()
  })
})

// ---------------------------------------------------------------------------
// 3. Valid/invalid funnel ordering
// ---------------------------------------------------------------------------

describe('FunnelPane – valid/invalid funnel ordering', () => {
  test('valid_funnels_appear_before_invalid_in_selector', async () => {
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_MIXED })
    renderFunnelPane()

    await waitFor(() => {
      expect(screen.getByTestId('funnel-select')).toBeInTheDocument()
    })

    const select = screen.getByTestId('funnel-select')
    const options = Array.from(select.querySelectorAll('option'))
    const validOptions = options.filter(o => !o.disabled && o.value !== '')
    const invalidOptions = options.filter(o => o.disabled)

    const maxValidIdx = Math.max(...validOptions.map(o => options.indexOf(o)))
    const minInvalidIdx = Math.min(...invalidOptions.map(o => options.indexOf(o)))
    expect(maxValidIdx).toBeLessThan(minInvalidIdx)
  })

  test('valid_funnel_names_appear_in_selector', async () => {
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_MIXED })
    renderFunnelPane()

    await waitFor(() => {
      expect(screen.getByTestId('funnel-select')).toBeInTheDocument()
    })

    const select = screen.getByTestId('funnel-select')
    expect(select.innerHTML).toContain('Valid Funnel A')
    expect(select.innerHTML).toContain('Valid Funnel C')
  })

  test('invalid_funnels_are_disabled_in_selector', async () => {
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_MIXED })
    renderFunnelPane()

    await waitFor(() => {
      expect(screen.getByTestId('funnel-select')).toBeInTheDocument()
    })

    const select = screen.getByTestId('funnel-select')
    const options = Array.from(select.querySelectorAll('option'))
    const invalidOption = options.find(o => o.textContent.includes('Invalid Funnel B'))
    expect(invalidOption).toBeInTheDocument()
    expect(invalidOption.disabled).toBe(true)
  })
})

// ---------------------------------------------------------------------------
// 4. Run button behavior  (Issue #6 fix)
// ---------------------------------------------------------------------------

describe('FunnelPane – run button behavior', () => {
  test('run_button_disabled_when_no_funnel_selected', async () => {
    listFunnels.mockResolvedValue({ funnels: [] })
    renderFunnelPane()
    await waitFor(() => {
      expect(screen.getByTestId('funnel-run-button')).toBeDisabled()
    })
  })

  test('run_button_enabled_when_valid_funnel_selected', async () => {
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()

    await waitFor(() => {
      expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled()
    })
  })

  test('run_button_disabled_while_running', async () => {
    // Make runFunnel slow to resolve so we can check the in-flight state
    let resolveRun
    runFunnel.mockReturnValue(new Promise(res => { resolveRun = res }))
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()

    await waitFor(() => expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled())

    fireEvent.click(screen.getByTestId('funnel-run-button'))

    // While running, button should be disabled
    await waitFor(() => {
      expect(screen.getByTestId('funnel-run-button')).toBeDisabled()
    })

    // Resolve the run
    resolveRun(MOCK_RUN_RESULT)
    await waitFor(() => {
      expect(screen.getByTestId('funnel-results')).toBeInTheDocument()
    })
  })

  test('run_button_calls_run_api_with_funnel_id', async () => {
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()

    await waitFor(() => {
      expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled()
    })

    fireEvent.click(screen.getByTestId('funnel-run-button'))

    await waitFor(() => {
      expect(runFunnel).toHaveBeenCalledWith(1)
    })
  })
})

// ---------------------------------------------------------------------------
// 5. Invalid funnel UX  (Issue #10)
// ---------------------------------------------------------------------------

describe('FunnelPane – invalid funnel UX', () => {
  test('run_button_disabled_when_only_invalid_funnels_exist', async () => {
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_INVALID_ONLY })
    renderFunnelPane()

    await waitFor(() => {
      expect(screen.getByTestId('funnel-run-button')).toBeDisabled()
    })
  })

  test('invalid_funnel_notice_appears_when_invalid_is_selected', async () => {
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_INVALID_ONLY })
    renderFunnelPane()

    await waitFor(() => {
      expect(screen.getByTestId('funnel-select')).toBeInTheDocument()
    })

    // Manually select the invalid funnel
    fireEvent.change(screen.getByTestId('funnel-select'), { target: { value: '1' } })

    await waitFor(() => {
      expect(screen.getByTestId('funnel-invalid-notice')).toBeInTheDocument()
    })
  })
})

// ---------------------------------------------------------------------------
// 6. Results rendering
// ---------------------------------------------------------------------------

describe('FunnelPane – results rendering', () => {
  test('funnel_chart_renders_after_run', async () => {
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()

    await waitFor(() => expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled())
    fireEvent.click(screen.getByTestId('funnel-run-button'))

    await waitFor(() => {
      expect(screen.getByTestId('funnel-results')).toBeInTheDocument()
      expect(screen.getByTestId('funnel-chart')).toBeInTheDocument()
    })
  })

  test('funnel_bar_step0_is_always_100_pct_wide', async () => {
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    // MOCK_RUN_RESULT: step 0 = 100 users, conversion_pct 100.0
    renderFunnelPane()

    await waitFor(() => expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled())
    fireEvent.click(screen.getByTestId('funnel-run-button'))

    await waitFor(() => expect(screen.getByTestId('funnel-chart')).toBeInTheDocument())

    // cohort_id=1, stepIdx=0 → conversion_pct = 100.0 → width should be 100%
    const bar0 = screen.getByTestId('funnel-bar-1-0').querySelector('.funnel-bar-fill')
    expect(bar0.style.width).toBe('100%')
  })

  test('funnel_bar_subsequent_step_width_matches_conversion_pct', async () => {
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    // MOCK_RUN_RESULT: step 1 = 42 users, conversion_pct 42.0
    renderFunnelPane()

    await waitFor(() => expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled())
    fireEvent.click(screen.getByTestId('funnel-run-button'))

    await waitFor(() => expect(screen.getByTestId('funnel-chart')).toBeInTheDocument())

    // cohort_id=1, stepIdx=1 → conversion_pct = 42.0 → width should be 42%
    const bar1 = screen.getByTestId('funnel-bar-1-1').querySelector('.funnel-bar-fill')
    expect(bar1.style.width).toBe('42%')
  })

  test('funnel_two_cohorts_same_pct_render_identical_bar_widths', async () => {
    // Two cohorts with different volumes but identical conversion %
    const sameConvResult = {
      funnel_id: 1,
      funnel_name: 'Multi Cohort',
      steps: ['signup', 'purchase'],
      results: [
        {
          cohort_id: 1,
          cohort_name: 'Cohort A (1000 users)',
          steps: [
            { step: 0, event_name: 'signup', users: 1000, conversion_pct: 100.0, dropoff_pct: 0.0 },
            { step: 1, event_name: 'purchase', users: 700, conversion_pct: 70.0, dropoff_pct: 30.0 },
          ],
        },
        {
          cohort_id: 2,
          cohort_name: 'Cohort B (500 users)',
          steps: [
            { step: 0, event_name: 'signup', users: 500, conversion_pct: 100.0, dropoff_pct: 0.0 },
            { step: 1, event_name: 'purchase', users: 350, conversion_pct: 70.0, dropoff_pct: 30.0 },
          ],
        },
      ],
    }
    runFunnel.mockResolvedValue(sameConvResult)
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()

    await waitFor(() => expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled())
    fireEvent.click(screen.getByTestId('funnel-run-button'))

    await waitFor(() => expect(screen.getByTestId('funnel-chart')).toBeInTheDocument())

    // Both cohorts at step 1 have 70% conversion → both bars must be 70% wide
    const barA = screen.getByTestId('funnel-bar-1-1').querySelector('.funnel-bar-fill')
    const barB = screen.getByTestId('funnel-bar-2-1').querySelector('.funnel-bar-fill')
    expect(barA.style.width).toBe('70%')
    expect(barB.style.width).toBe('70%')
  })

  test('funnel_bar_zero_step0_renders_zero_width', async () => {
    runFunnel.mockResolvedValue(MOCK_RUN_RESULT_ZERO_USERS)
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()

    await waitFor(() => expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled())
    fireEvent.click(screen.getByTestId('funnel-run-button'))

    await waitFor(() => expect(screen.getByTestId('funnel-chart')).toBeInTheDocument())

    const bar0 = screen.getByTestId('funnel-bar-1-0').querySelector('.funnel-bar-fill')
    // conversion_pct = 0 → barWidth = 0 → width 0%, minWidth 0px
    expect(bar0.style.width).toBe('0%')
    expect(bar0.style.minWidth).toBe('0px')
  })

  test('funnel_bar_null_conversion_pct_renders_safely', async () => {
    // Bad data: conversion_pct is null
    const nullPctResult = {
      funnel_id: 1, funnel_name: 'Bad Data', steps: ['signup', 'purchase'],
      results: [{ cohort_id: 1, cohort_name: 'All Users', steps: [
        { step: 0, event_name: 'signup', users: 100, conversion_pct: null, dropoff_pct: 0 },
        { step: 1, event_name: 'purchase', users: 50, conversion_pct: null, dropoff_pct: 0 },
      ]}],
    }
    runFunnel.mockResolvedValue(nullPctResult)
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()

    await waitFor(() => expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled())
    fireEvent.click(screen.getByTestId('funnel-run-button'))

    await waitFor(() => expect(screen.getByTestId('funnel-chart')).toBeInTheDocument())

    // null → Number(null) = 0 → barWidth = 0 → no NaN, no crash
    const bar = screen.getByTestId('funnel-bar-1-0').querySelector('.funnel-bar-fill')
    expect(bar.style.width).toBe('0%')
    expect(bar.style.width).not.toContain('NaN')
  })

  test('funnel_bar_overflow_pct_clamped_to_100', async () => {
    // Backend rounding edge: conversion_pct = 100.4 (> 100)
    const overflowResult = {
      funnel_id: 1, funnel_name: 'Overflow', steps: ['signup', 'purchase'],
      results: [{ cohort_id: 1, cohort_name: 'All Users', steps: [
        { step: 0, event_name: 'signup', users: 100, conversion_pct: 100.4, dropoff_pct: 0 },
        { step: 1, event_name: 'purchase', users: 80, conversion_pct: 80.0, dropoff_pct: 20 },
      ]}],
    }
    runFunnel.mockResolvedValue(overflowResult)
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()

    await waitFor(() => expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled())
    fireEvent.click(screen.getByTestId('funnel-run-button'))

    await waitFor(() => expect(screen.getByTestId('funnel-chart')).toBeInTheDocument())

    // 100.4 → clamped to 100 → width 100%
    const bar = screen.getByTestId('funnel-bar-1-0').querySelector('.funnel-bar-fill')
    expect(bar.style.width).toBe('100%')
  })

  test('funnel_bar_tiny_pct_has_2px_min_width', async () => {
    // 0.1% conversion — bar must still be visible (2px minWidth)
    const tinyResult = {
      funnel_id: 1, funnel_name: 'Tiny', steps: ['signup', 'purchase'],
      results: [{ cohort_id: 1, cohort_name: 'All Users', steps: [
        { step: 0, event_name: 'signup', users: 1000, conversion_pct: 100.0, dropoff_pct: 0 },
        { step: 1, event_name: 'purchase', users: 1, conversion_pct: 0.1, dropoff_pct: 99.9 },
      ]}],
    }
    runFunnel.mockResolvedValue(tinyResult)
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()

    await waitFor(() => expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled())
    fireEvent.click(screen.getByTestId('funnel-run-button'))

    await waitFor(() => expect(screen.getByTestId('funnel-chart')).toBeInTheDocument())

    const bar = screen.getByTestId('funnel-bar-1-1').querySelector('.funnel-bar-fill')
    expect(bar.style.width).toBe('0.1%')
    expect(bar.style.minWidth).toBe('2px')   // visible even at tiny %
  })

  test('funnel_bar_width_matches_displayed_label_exactly', async () => {
    // Ensure width (1-decimal rounded) matches what toFixed(1) shows in the label
    const precisionResult = {
      funnel_id: 1, funnel_name: 'Precision', steps: ['signup', 'purchase'],
      results: [{ cohort_id: 1, cohort_name: 'All Users', steps: [
        { step: 0, event_name: 'signup', users: 1000, conversion_pct: 100.0, dropoff_pct: 0 },
        // 72.123456% — bar should show 72.1%, label should show 72.1%
        { step: 1, event_name: 'purchase', users: 721, conversion_pct: 72.123456, dropoff_pct: 27.9 },
      ]}],
    }
    runFunnel.mockResolvedValue(precisionResult)
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()

    await waitFor(() => expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled())
    fireEvent.click(screen.getByTestId('funnel-run-button'))

    await waitFor(() => expect(screen.getByTestId('funnel-chart')).toBeInTheDocument())

    const bar = screen.getByTestId('funnel-bar-1-1').querySelector('.funnel-bar-fill')
    // Bar width rounded to 1 decimal = 72.1%
    expect(bar.style.width).toBe('72.1%')
    // Label also shows 72.1% via toFixed(1) — both match
    expect(screen.getByTestId('funnel-bar-1-1')).toHaveTextContent('72.1%')
  })

  test('funnel_table_renders_with_correct_cohort_data', async () => {
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()

    await waitFor(() => expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled())
    fireEvent.click(screen.getByTestId('funnel-run-button'))

    await waitFor(() => {
      expect(screen.getByTestId('funnel-table')).toBeInTheDocument()
    })

    expect(screen.getByTestId('funnel-table')).toHaveTextContent('All Users')
    expect(screen.getByTestId('funnel-table')).toHaveTextContent('100')
    expect(screen.getByTestId('funnel-table')).toHaveTextContent('42')
  })

  test('funnel_zero_users_shows_empty_message', async () => {
    // Issue #8
    runFunnel.mockResolvedValue(MOCK_RUN_RESULT_ZERO_USERS)
    listFunnels.mockResolvedValue({ funnels: MOCK_FUNNELS_VALID_ONLY })
    renderFunnelPane()

    await waitFor(() => expect(screen.getByTestId('funnel-run-button')).not.toBeDisabled())
    fireEvent.click(screen.getByTestId('funnel-run-button'))

    await waitFor(() => {
      expect(screen.getByTestId('funnel-no-users')).toBeInTheDocument()
    })
  })
})

// ---------------------------------------------------------------------------
// 7. Funnel builder modal
// ---------------------------------------------------------------------------

describe('FunnelPane – funnel builder modal', () => {
  test('builder_modal_opens_on_new_funnel_click', async () => {
    renderFunnelPane()
    await waitFor(() => expect(screen.getByTestId('funnel-new-button')).toBeInTheDocument())

    fireEvent.click(screen.getByTestId('funnel-new-button'))
    expect(screen.getByTestId('funnel-builder-modal')).toBeInTheDocument()
  })

  test('builder_modal_shows_two_steps_by_default', async () => {
    renderFunnelPane()
    await waitFor(() => expect(screen.getByTestId('funnel-new-button')).toBeInTheDocument())

    fireEvent.click(screen.getByTestId('funnel-new-button'))

    expect(screen.getByTestId('funnel-step-0')).toBeInTheDocument()
    expect(screen.getByTestId('funnel-step-1')).toBeInTheDocument()
    expect(screen.queryByTestId('funnel-step-2')).not.toBeInTheDocument()
    expect(screen.getByText('No time restriction between steps (lifetime conversion)')).toBeInTheDocument()
  })

  test('builder_adds_step_on_add_step_click', async () => {
    renderFunnelPane()
    await waitFor(() => expect(screen.getByTestId('funnel-new-button')).toBeInTheDocument())

    fireEvent.click(screen.getByTestId('funnel-new-button'))
    fireEvent.click(screen.getByTestId('funnel-add-step'))

    expect(screen.getByTestId('funnel-step-2')).toBeInTheDocument()
  })

  test('builder_allows_up_to_ten_steps', async () => {
    renderFunnelPane()
    await waitFor(() => expect(screen.getByTestId('funnel-new-button')).toBeInTheDocument())
    fireEvent.click(screen.getByTestId('funnel-new-button'))

    for (let i = 0; i < 8; i++) {
      fireEvent.click(screen.getByTestId('funnel-add-step'))
    }

    expect(screen.getByTestId('funnel-step-9')).toBeInTheDocument()
    expect(screen.queryByTestId('funnel-add-step')).not.toBeInTheDocument()
  })

  test('builder_shows_error_when_name_empty_on_save', async () => {
    renderFunnelPane()
    await waitFor(() => expect(screen.getByTestId('funnel-new-button')).toBeInTheDocument())

    fireEvent.click(screen.getByTestId('funnel-new-button'))
    fireEvent.click(screen.getByTestId('funnel-save-button'))

    expect(screen.getByTestId('funnel-builder-error')).toHaveTextContent('Funnel name is required')
    expect(createFunnel).not.toHaveBeenCalled()
  })

  test('builder_clears_filters_when_event_changes', async () => {
    // Mock getEventProperties to resolve immediately so no pending async state
    const { getEventProperties } = await import('../src/api')
    getEventProperties.mockResolvedValue({ properties: ['category', 'query'] })

    renderFunnelPane(MOCK_EVENTS_STR)
    await waitFor(() => expect(screen.getByTestId('funnel-new-button')).toBeInTheDocument())

    fireEvent.click(screen.getByTestId('funnel-new-button'))

    // Select event for step 0
    fireEvent.change(screen.getByTestId('funnel-step-event-0'), { target: { value: 'signup' } })

    // Wait for any async property loading to settle
    await waitFor(() => expect(screen.getByTestId('funnel-add-filter-0')).not.toBeDisabled())

    // Add a filter
    fireEvent.click(screen.getByTestId('funnel-add-filter-0'))

    await waitFor(() => expect(screen.getByTestId('funnel-filter-0-0')).toBeInTheDocument())

    // Change event — filters should be cleared
    fireEvent.change(screen.getByTestId('funnel-step-event-0'), { target: { value: 'purchase' } })

    await waitFor(() => {
      expect(screen.queryByTestId('funnel-filter-0-0')).not.toBeInTheDocument()
    })
  })

  test('builder_sends_custom_conversion_window_in_payload', async () => {
    renderFunnelPane(MOCK_EVENTS_STR)
    await waitFor(() => expect(screen.getByTestId('funnel-new-button')).toBeInTheDocument())
    fireEvent.click(screen.getByTestId('funnel-new-button'))

    fireEvent.change(screen.getByTestId('funnel-name-input'), { target: { value: 'Window Funnel' } })
    fireEvent.change(screen.getByTestId('funnel-step-event-0'), { target: { value: 'signup' } })
    fireEvent.change(screen.getByTestId('funnel-step-event-1'), { target: { value: 'purchase' } })
    fireEvent.change(screen.getByTestId('funnel-conversion-window-mode'), { target: { value: 'custom' } })
    fireEvent.change(screen.getByTestId('funnel-conversion-window-value'), { target: { value: '15' } })
    fireEvent.click(screen.getByTestId('funnel-save-button'))

    await waitFor(() => {
      expect(createFunnel).toHaveBeenCalledWith(expect.objectContaining({
        conversion_window: { value: 15, unit: 'minute' },
      }))
    })
  })

  test('builder_shows_error_when_conversion_window_exceeds_upper_bound', async () => {
    renderFunnelPane(MOCK_EVENTS_STR)
    await waitFor(() => expect(screen.getByTestId('funnel-new-button')).toBeInTheDocument())
    fireEvent.click(screen.getByTestId('funnel-new-button'))

    fireEvent.change(screen.getByTestId('funnel-name-input'), { target: { value: 'Window Limit Funnel' } })
    fireEvent.change(screen.getByTestId('funnel-step-event-0'), { target: { value: 'signup' } })
    fireEvent.change(screen.getByTestId('funnel-step-event-1'), { target: { value: 'purchase' } })
    fireEvent.change(screen.getByTestId('funnel-conversion-window-mode'), { target: { value: 'custom' } })
    fireEvent.change(screen.getByTestId('funnel-conversion-window-value'), { target: { value: '10081' } })
    fireEvent.click(screen.getByTestId('funnel-save-button'))

    expect(screen.getByTestId('funnel-builder-error')).toHaveTextContent(
      'Conversion window cannot exceed 7 days (10080 minutes)',
    )
    expect(createFunnel).not.toHaveBeenCalled()
  })

  test('builder_sends_explicit_step_order_in_payload', async () => {
    renderFunnelPane(MOCK_EVENTS_STR)
    await waitFor(() => expect(screen.getByTestId('funnel-new-button')).toBeInTheDocument())
    fireEvent.click(screen.getByTestId('funnel-new-button'))
    fireEvent.click(screen.getByTestId('funnel-add-step'))

    fireEvent.change(screen.getByTestId('funnel-name-input'), { target: { value: 'Reordered Funnel' } })
    fireEvent.change(screen.getByTestId('funnel-step-event-0'), { target: { value: 'signup' } })
    fireEvent.change(screen.getByTestId('funnel-step-event-1'), { target: { value: 'search' } })
    fireEvent.change(screen.getByTestId('funnel-step-event-2'), { target: { value: 'purchase' } })

    fireEvent.click(screen.getByTestId('funnel-save-button'))
    await waitFor(() => expect(createFunnel).toHaveBeenCalled())
    expect(createFunnel.mock.calls[0][0].steps.map(s => s.step_order)).toEqual([
      0, 1, 2,
    ])
  })
})
