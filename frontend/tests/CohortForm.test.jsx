import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { vi } from 'vitest'
import CohortForm from '../src/components/CohortForm'
import * as api from '../src/api'

vi.mock('../src/api', () => ({
  createSavedCohort: vi.fn(),
  updateSavedCohort: vi.fn(),
  createCohort: vi.fn(),
  estimateCohort: vi.fn(),
  listEvents: vi.fn(),
  getColumns: vi.fn(),
  getColumnValues: vi.fn()
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

describe('CohortForm auto-add logic', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    api.listEvents.mockResolvedValue({ events: ['purchase', 'signup'] })
    api.getColumns.mockResolvedValue({ columns: [{ name: 'country', category: 'property', data_type: 'TEXT' }] })
    api.estimateCohort.mockResolvedValue({ estimated_users: 100 })
  })

  it('auto-adds valid saved cohort to workspace on create', async () => {
    api.createSavedCohort.mockResolvedValue({ id: 'saved-123', is_valid: true })
    api.createCohort.mockResolvedValue({ cohort_id: 1 })
    
    const onSave = vi.fn()
    const onCancel = vi.fn()

    render(
      <CohortForm mode="create_saved" onSave={onSave} onCancel={onCancel} />
    )

    // Wait for initial load
    await waitFor(() => expect(screen.getByTestId('mock-searchable-select')).toBeInTheDocument())

    // Set a name and submit
    fireEvent.change(screen.getByPlaceholderText('Cohort name (optional, defaults to description)'), { target: { value: 'Test Auto Add' } })
    fireEvent.click(screen.getByText('Save Cohort'))

    await waitFor(() => {
      expect(api.createSavedCohort).toHaveBeenCalledWith(expect.objectContaining({
        name: 'Test Auto Add',
        conditions: expect.arrayContaining([
          expect.objectContaining({ event_name: 'purchase' })
        ])
      }))
    })

    // Should automatically call createCohort with source_saved_id
    await waitFor(() => {
      expect(api.createCohort).toHaveBeenCalledWith(expect.objectContaining({
        name: 'Test Auto Add',
        source_saved_id: 'saved-123'
      }))
    })

    expect(onSave).toHaveBeenCalled()
  })

  it('does NOT auto-add if editing an existing saved cohort', async () => {
    api.updateSavedCohort.mockResolvedValue({ id: 'saved-123', is_valid: true })
    
    const onSave = vi.fn()
    const initialData = {
      id: 'saved-123',
      name: 'Existing',
      definition: {
        logic_operator: 'AND',
        join_type: 'condition_met',
        conditions: [{ event_name: 'signup', min_event_count: 1 }]
      }
    }

    render(
      <CohortForm mode="edit_saved" initialData={initialData} onSave={onSave} onCancel={vi.fn()} />
    )

    await waitFor(() => expect(screen.getByTestId('mock-searchable-select')).toBeInTheDocument())

    fireEvent.click(screen.getByText('Save Cohort'))

    await waitFor(() => {
      expect(api.updateSavedCohort).toHaveBeenCalled()
    })

    expect(api.createCohort).not.toHaveBeenCalled()
    expect(onSave).toHaveBeenCalled()
  })

  it('does NOT auto-add if the new saved cohort is invalid for the dataset', async () => {
    api.createSavedCohort.mockResolvedValue({ id: 'saved-999', is_valid: false, errors: [] })
    
    const onSave = vi.fn()

    render(
      <CohortForm mode="create_saved" onSave={onSave} onCancel={vi.fn()} />
    )

    await waitFor(() => expect(screen.getByTestId('mock-searchable-select')).toBeInTheDocument())

    fireEvent.click(screen.getByText('Save Cohort'))

    await waitFor(() => {
      expect(api.createSavedCohort).toHaveBeenCalled()
    })

    // Should show error and NOT call createCohort or onSave
    await waitFor(() => {
      expect(screen.getByText(/invalid for this dataset/i)).toBeInTheDocument()
    })

    expect(api.createCohort).not.toHaveBeenCalled()
    expect(onSave).not.toHaveBeenCalled()
  })

  it('prefills data correctly when initialData is provided in create mode', async () => {
    const initialData = {
      name: 'Copy of Original',
      definition: {
        logic_operator: 'OR',
        join_type: 'first_event',
        conditions: [{ event_name: 'signup', min_event_count: 5 }]
      }
    }

    render(
      <CohortForm mode="create_saved" initialData={initialData} onSave={vi.fn()} onCancel={vi.fn()} />
    )

    await waitFor(() => expect(screen.getByTestId('mock-searchable-select')).toBeInTheDocument())

    expect(screen.getByPlaceholderText('Cohort name (optional, defaults to description)').value).toBe('Copy of Original')
    expect(screen.getByDisplayValue('ANY conditions (OR)')).toBeInTheDocument()
    expect(screen.getByDisplayValue('Join on first qualifying event')).toBeInTheDocument()
    
    // Check if the condition event is set
    expect(screen.getByDisplayValue('signup')).toBeInTheDocument()
    expect(screen.getByDisplayValue('5')).toBeInTheDocument()
  })
})

describe('CohortForm batch creation', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    api.listEvents.mockResolvedValue({ events: ['purchase', 'signup'] })
    api.getColumns.mockResolvedValue({ columns: [{ name: 'country', category: 'property', data_type: 'TEXT' }] })
    api.estimateCohort.mockResolvedValue({ estimated_users: 100 })
    api.createSavedCohort.mockResolvedValue({ id: 'saved-123', is_valid: true })
    api.createCohort.mockResolvedValue({ cohort_id: 1 })
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('toggles batch mode and updates layout', async () => {
    render(<CohortForm mode="create_saved" onSave={vi.fn()} onCancel={vi.fn()} />)
    await waitFor(() => expect(screen.getByText('Save Cohort')).toBeInTheDocument())

    const saveButton = screen.getByText('Save Cohort')
    const toggle = screen.getByLabelText('Multi-Create')
    
    // Initially OFF
    expect(screen.queryByText('Reset')).not.toBeInTheDocument()
    expect(saveButton.style.width).toBe('80%')

    // Toggle ON
    fireEvent.click(toggle)
    expect(screen.getByText('Reset')).toBeInTheDocument()
    expect(saveButton.style.width).toBe('60%')
  })

  it('keeps modal open and increments name on save when batch mode is ON', async () => {
    const onSave = vi.fn()
    const onCancel = vi.fn()
    render(<CohortForm mode="create_saved" onSave={onSave} onCancel={onCancel} />)
    
    await waitFor(() => expect(screen.getByLabelText('Multi-Create')).toBeInTheDocument())
    fireEvent.click(screen.getByLabelText('Multi-Create'))
    
    const nameInput = screen.getByPlaceholderText(/Cohort name/)
    fireEvent.change(nameInput, { target: { value: 'Batch Test' } })
    
    fireEvent.click(screen.getByText('Save Cohort'))

    await waitFor(() => expect(api.createSavedCohort).toHaveBeenCalled())
    expect(onSave).toHaveBeenCalled()
    expect(onCancel).not.toHaveBeenCalled()
    
    // Name should increment
    expect(nameInput.value).toBe('Batch Test (1)')
    
    // Second save
    fireEvent.click(screen.getByText('Save Cohort'))
    await waitFor(() => expect(api.createSavedCohort).toHaveBeenCalledTimes(2))
    expect(nameInput.value).toBe('Batch Test (2)')
  })

  it('resets form but preserves toggle when Reset is clicked', async () => {
    render(<CohortForm mode="create_saved" onSave={vi.fn()} onCancel={vi.fn()} />)
    await waitFor(() => expect(screen.getByLabelText('Multi-Create')).toBeInTheDocument())
    
    fireEvent.click(screen.getByLabelText('Multi-Create'))
    const nameInput = screen.getByPlaceholderText(/Cohort name/)
    fireEvent.change(nameInput, { target: { value: 'To Be Reset' } })
    
    fireEvent.click(screen.getByText('Reset'))
    
    expect(nameInput.value).toBe('')
    expect(screen.getByLabelText('Multi-Create')).toBeChecked()
  })

  it('does NOT increment name on API failure', async () => {
    api.createSavedCohort.mockRejectedValue(new Error('API Fail'))
    render(<CohortForm mode="create_saved" onSave={vi.fn()} onCancel={vi.fn()} />)
    
    await waitFor(() => expect(screen.getByLabelText('Multi-Create')).toBeInTheDocument())
    fireEvent.click(screen.getByLabelText('Multi-Create'))
    
    const nameInput = screen.getByPlaceholderText(/Cohort name/)
    fireEvent.change(nameInput, { target: { value: 'Fail Test' } })
    
    fireEvent.click(screen.getByText('Save Cohort'))
    
    await waitFor(() => expect(screen.getByText('API Fail')).toBeInTheDocument())
    expect(nameInput.value).toBe('Fail Test')
  })

  it('shows and hides toast notification', async () => {
    render(<CohortForm mode="create_saved" onSave={vi.fn()} onCancel={vi.fn()} />)
    await waitFor(() => expect(screen.getByText('Save Cohort')).toBeInTheDocument())
    
    fireEvent.change(screen.getByPlaceholderText(/Cohort name/), { target: { value: 'Toast Test' } })
    fireEvent.click(screen.getByText('Save Cohort'))
    
    await waitFor(() => expect(screen.getByText(/Cohort "Toast Test" created/)).toBeInTheDocument())
    
    // Advance time
    vi.advanceTimersByTime(2000)
    expect(screen.queryByText(/Cohort "Toast Test" created/)).not.toBeInTheDocument()
  })

  it('handles empty name correctly (no increment)', async () => {
    render(<CohortForm mode="create_saved" onSave={vi.fn()} onCancel={vi.fn()} />)
    await waitFor(() => expect(screen.getByLabelText('Multi-Create')).toBeInTheDocument())
    fireEvent.click(screen.getByLabelText('Multi-Create'))
    
    fireEvent.click(screen.getByText('Save Cohort'))
    
    await waitFor(() => expect(api.createSavedCohort).toHaveBeenCalled())
    expect(screen.getByPlaceholderText(/Cohort name/).value).toBe('')
  })

  it('increments correctly with rapid clicks using functional updates', async () => {
    render(<CohortForm mode="create_saved" onSave={vi.fn()} onCancel={vi.fn()} />)
    await waitFor(() => expect(screen.getByLabelText('Multi-Create')).toBeInTheDocument())
    fireEvent.click(screen.getByLabelText('Multi-Create'))
    
    const nameInput = screen.getByPlaceholderText(/Cohort name/)
    fireEvent.change(nameInput, { target: { value: 'Rapid' } })
    
    // Mock multiple saves resolving
    fireEvent.click(screen.getByText('Save Cohort'))
    fireEvent.click(screen.getByText('Save Cohort'))
    
    await waitFor(() => expect(api.createSavedCohort).toHaveBeenCalledTimes(2))
    expect(nameInput.value).toBe('Rapid (2)')
  })
})

