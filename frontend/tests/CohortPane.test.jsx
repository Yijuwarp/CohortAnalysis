import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { vi } from 'vitest'
import CohortPane from '../src/components/CohortPane'
import * as api from '../src/api'

vi.mock('../src/api', () => ({
  listCohorts: vi.fn(),
  getSavedCohorts: vi.fn(),
  createCohort: vi.fn(),
  deleteCohort: vi.fn(),
  toggleCohortHide: vi.fn(),
  randomSplitCohort: vi.fn(),
  createSavedCohort: vi.fn(),
  updateSavedCohort: vi.fn(),
  estimateCohort: vi.fn(),
  listEvents: vi.fn(),
  getColumns: vi.fn(),
  getColumnValues: vi.fn()
}))

// Mock CohortForm and SavedCohortsPanel to simplify integration test
vi.mock('../src/components/CohortForm', () => ({
  default: ({ mode, initialData, onCancel, onSave }) => (
    <div data-testid="mock-cohort-form">
      <h3>{mode === 'edit_saved' ? 'Edit Saved Cohort' : 'Create Saved Cohort'}</h3>
      <input 
        placeholder="Cohort name (optional, defaults to description)" 
        defaultValue={initialData?.name || ''} 
      />
      <div data-testid="logic-op">{initialData?.definition?.logic_operator}</div>
      <div data-testid="join-type">{initialData?.definition?.join_type}</div>
      <div data-testid="condition-count">{initialData?.definition?.conditions?.length}</div>
      <div data-testid="first-condition-event">{initialData?.definition?.conditions?.[0]?.event_name}</div>
      <div data-testid="first-condition-count">{initialData?.definition?.conditions?.[0]?.min_event_count}</div>

      <button onClick={onCancel}>Cancel</button>
      <button onClick={onSave}>Save Cohort</button>
    </div>
  )
}))

vi.mock('../src/components/SearchableSelect', () => ({
  default: ({ value, onChange, placeholder, options }) => (
    <select data-testid="mock-searchable-select" value={value || ''} onChange={e => onChange(e.target.value)}>
      <option value="">{placeholder}</option>
      {options && options.map(o => (
        <option key={o.value || o} value={o.value || o}>
          {o.label || o}
        </option>
      ))}
    </select>
  )
}))

describe('CohortPane Duplicate Flow', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    api.listCohorts.mockResolvedValue({ cohorts: [] })
    api.getSavedCohorts.mockResolvedValue([
      {
        id: 'saved-1',
        name: 'Original Cohort',
        is_valid: true,
        definition: {
          logic_operator: 'OR',
          join_type: 'first_event',
          conditions: [{ event_name: 'signup', min_event_count: 5 }]
        }
      }
    ])
    api.listEvents.mockResolvedValue({ events: ['signup'] })
    api.getColumns.mockResolvedValue({ columns: [] })
  })

  it('duplicates cohort and opens form with full prefilled data', async () => {
    render(<CohortPane refreshToken={1} onCohortsChanged={vi.fn()} />)

    // Wait for initial load
    await waitFor(() => expect(api.getSavedCohorts).toHaveBeenCalled())

    // open saved cohorts panel
    fireEvent.click(screen.getByText('Manage'))

    // verify panel is open
    expect(screen.getByText('Manage Saved Cohorts')).toBeInTheDocument()

    // click duplicate icon
    const duplicateButtons = screen.getAllByTitle('Duplicate saved cohort')
    fireEvent.click(duplicateButtons[0])

    // verify modal transition (wrapped in setTimeout in code, so we wait)
    await waitFor(() => expect(screen.getByTestId('mock-cohort-form')).toBeInTheDocument())

    // 1. Panel is closed
    expect(screen.queryByText('Manage Saved Cohorts')).not.toBeInTheDocument()

    // 2. Name is prefilled correctly (no timestamp)
    expect(screen.getByDisplayValue('Copy of Original Cohort')).toBeInTheDocument()

    // 3. Definition fields are preserved
    expect(screen.getByTestId('logic-op').textContent).toBe('OR')
    expect(screen.getByTestId('join-type').textContent).toBe('first_event')
    expect(screen.getByTestId('condition-count').textContent).toBe('1')
    expect(screen.getByTestId('first-condition-event').textContent).toBe('signup')
    expect(screen.getByTestId('first-condition-count').textContent).toBe('5')
  })
})
