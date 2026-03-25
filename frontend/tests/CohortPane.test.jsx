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
  splitCohort: vi.fn(),
  previewSplit: vi.fn(),
  createSavedCohort: vi.fn(),
  updateSavedCohort: vi.fn(),
  estimateCohort: vi.fn(),
  listEvents: vi.fn(),
  getColumns: vi.fn(),
  getColumnValues: vi.fn(),
  getCohortDetail: vi.fn()
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

vi.mock('../src/components/CohortSplitModal', () => ({
  default: ({ cohort, onClose, onSplitDone }) => (
    <div data-testid="mock-split-modal">
      <h3>Split {cohort.cohort_name}</h3>
      <button onClick={onClose}>Cancel</button>
      <button onClick={async () => {
        await api.splitCohort(cohort.cohort_id, { type: 'random', random: { num_groups: 2 } });
        onSplitDone();
      }}>Confirm Split</button>
    </div>
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

describe('CohortPane Split Flow', () => {
  const mockParent = {
    cohort_id: 'p1',
    cohort_name: 'Parent Cohort',
    size: 100,
    is_active: true,
    has_splits: false,
    definition: { logic_operator: 'AND', conditions: [] }
  }

  const mockChild = {
    cohort_id: 'c1',
    cohort_name: 'Child 1',
    size: 50,
    is_active: true,
    split_parent_cohort_id: 'p1',
    definition: { logic_operator: 'AND', conditions: [] }
  }

  beforeEach(() => {
    vi.clearAllMocks()
    api.listCohorts.mockResolvedValue({ cohorts: [mockParent] })
    api.getSavedCohorts.mockResolvedValue([])
    api.getCohortDetail.mockImplementation(id => {
      if (id === 'p1') return Promise.resolve(mockParent)
      if (id === 'c1') return Promise.resolve(mockChild)
      return Promise.reject('Not found')
    })
  })

  it('opens split modal when clicking split on unsplit cohort', async () => {
    render(<CohortPane refreshToken={1} onCohortsChanged={vi.fn()} />)
    await waitFor(() => expect(api.listCohorts).toHaveBeenCalled())

    // Find and click the split button (the one with "Split cohort" title)
    const splitBtn = screen.getByTitle('Split cohort')
    fireEvent.click(splitBtn)

    // Verify modal is open
    expect(screen.getByTestId('mock-split-modal')).toBeInTheDocument()
    expect(screen.getByText('Split Parent Cohort')).toBeInTheDocument()

    // Ensure NO API call was made yet
    expect(api.splitCohort).not.toHaveBeenCalled()
    expect(api.randomSplitCohort).not.toHaveBeenCalled()
  })

  it('calls splitCohort API only after modal confirmation', async () => {
    render(<CohortPane refreshToken={1} onCohortsChanged={vi.fn()} />)
    await waitFor(() => expect(api.listCohorts).toHaveBeenCalled())

    fireEvent.click(screen.getByTitle('Split cohort'))
    
    // Confirm in modal
    fireEvent.click(screen.getByText('Confirm Split'))

    await waitFor(() => expect(api.splitCohort).toHaveBeenCalledWith('p1', expect.anything()))
    expect(api.randomSplitCohort).not.toHaveBeenCalled()
  })

  it('removes children if cohort already split', async () => {
    // Modify mock to show it has splits
    const splitParent = { ...mockParent, has_splits: true }
    api.listCohorts.mockResolvedValue({ cohorts: [splitParent, mockChild] })
    api.getCohortDetail.mockImplementation(id => {
      if (id === 'p1') return Promise.resolve(splitParent)
      if (id === 'c1') return Promise.resolve(mockChild)
      return Promise.reject('Not found')
    })

    render(<CohortPane refreshToken={1} onCohortsChanged={vi.fn()} />)
    await waitFor(() => expect(api.listCohorts).toHaveBeenCalled())

    const removeSplitBtn = screen.getByTitle('Remove split')
    fireEvent.click(removeSplitBtn)

    // Should call deleteCohort for the child
    await waitFor(() => expect(api.deleteCohort).toHaveBeenCalledWith('c1'))
    
    // Modal should NOT be open
    expect(screen.queryByTestId('mock-split-modal')).not.toBeInTheDocument()
  })
})
