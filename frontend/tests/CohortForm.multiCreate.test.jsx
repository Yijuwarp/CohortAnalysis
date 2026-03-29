import React from 'react'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import CohortForm from '../src/components/CohortForm'
import * as api from '../src/api'
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest'

vi.mock('../src/api', async () => {
  const actual = await vi.importActual('../src/api')
  return {
    ...actual,
    listEvents: vi.fn(),
    getColumns: vi.fn(),
    estimateCohort: vi.fn(),
    createSavedCohort: vi.fn(),
    createCohort: vi.fn(),
  }
})

describe('CohortForm multi-create state preservation', () => {
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

  it('preserves form state when creating multiple cohorts', async () => {
    const onSave = vi.fn()
    render(<CohortForm mode="create_saved" onSave={onSave} onCancel={vi.fn()} />)
    
    await waitFor(() => expect(screen.getByLabelText('Multi-Create')).toBeInTheDocument())
    
    // 1. Fill form with specific values
    const nameInput = screen.getByPlaceholderText(/Cohort name/)
    fireEvent.change(nameInput, { target: { value: 'Base Cohort' } })
    
    // Change Logic Operator to OR
    const logicSelect = screen.getByDisplayValue(/ALL conditions/)
    fireEvent.change(logicSelect, { target: { value: 'OR' } })
    
    // Enable Multi-Create
    fireEvent.click(screen.getByLabelText('Multi-Create'))
    
    // 2. Submit
    fireEvent.click(screen.getByText('Save Cohort'))
    
    await waitFor(() => expect(api.createSavedCohort).toHaveBeenCalled())
    
    // 3. Assertions
    // Name incremented
    expect(nameInput.value).toBe('Base Cohort (1)')
    
    // Logic operator preserved (OR)
    expect(logicSelect.value).toBe('OR')
    
    // Estimation reset (renders as '-')
    expect(screen.getByText('-')).toBeInTheDocument()
  })
})
