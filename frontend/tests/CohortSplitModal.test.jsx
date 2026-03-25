import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { vi } from 'vitest'
import CohortSplitModal from '../src/components/CohortSplitModal'
import * as api from '../src/api'

vi.mock('../src/api', () => ({
  getColumns: vi.fn(),
  getColumnValues: vi.fn(),
  splitCohort: vi.fn(),
  previewSplit: vi.fn()
}))

describe('CohortSplitModal Property Tab', () => {
  const mockCohort = { cohort_id: 'p1', cohort_name: 'Parent' }
  const mockColumns = [
    { name: 'country', data_type: 'TEXT' },
    { name: 'channel', data_type: 'TEXT' }
  ]

  beforeEach(() => {
    vi.clearAllMocks()
    api.getColumns.mockResolvedValue({ columns: mockColumns })
    api.getColumnValues.mockResolvedValue({ values: ['US', 'India'] })
    api.previewSplit.mockResolvedValue({ preview: [] })
  })

  it('renders property tab without crashing', async () => {
    render(<CohortSplitModal cohort={mockCohort} onClose={vi.fn()} onSplitDone={vi.fn()} />)

    // Wait for columns to load (on mount)
    await waitFor(() => expect(api.getColumns).toHaveBeenCalled())

    // Switch to Property Tab
    fireEvent.click(screen.getByText(/Property Split/i))

    // Check if label exists
    expect(screen.getByLabelText(/Property column/i)).toBeInTheDocument()
  })

  it('renders column names correctly in dropdown (not objects)', async () => {
    render(<CohortSplitModal cohort={mockCohort} onClose={vi.fn()} onSplitDone={vi.fn()} />)
    await waitFor(() => expect(api.getColumns).toHaveBeenCalled())
    
    fireEvent.click(screen.getByText(/Property Split/i))

    const select = screen.getByLabelText(/Property column/i)
    const options = screen.getAllByRole('option')
    
    // Should show "country" and "channel", not "[object Object]"
    expect(options[0].textContent).toBe('country')
    expect(options[1].textContent).toBe('channel')
    expect(options[0].value).toBe('country')
  })

  it('loads values when column is selected', async () => {
    render(<CohortSplitModal cohort={mockCohort} onClose={vi.fn()} onSplitDone={vi.fn()} />)
    await waitFor(() => expect(api.getColumns).toHaveBeenCalled())
    
    fireEvent.click(screen.getByText(/Property Split/i))

    // Wait for default column values to load
    await waitFor(() => expect(api.getColumnValues).toHaveBeenCalledWith('country'))
    
    expect(screen.getByText('US')).toBeInTheDocument()
    expect(screen.getByText('India')).toBeInTheDocument()
  })

  it('updates values when column changes', async () => {
    api.getColumnValues.mockImplementation((col) => {
      if (col === 'channel') return Promise.resolve({ values: ['ads', 'email'] })
      return Promise.resolve({ values: ['US', 'India'] })
    })

    render(<CohortSplitModal cohort={mockCohort} onClose={vi.fn()} onSplitDone={vi.fn()} />)
    await waitFor(() => expect(api.getColumns).toHaveBeenCalled())
    
    fireEvent.click(screen.getByText(/Property Split/i))
    await waitFor(() => expect(api.getColumnValues).toHaveBeenCalledWith('country'))

    const select = screen.getByLabelText(/Property column/i)
    fireEvent.change(select, { target: { value: 'channel' } })

    await waitFor(() => expect(api.getColumnValues).toHaveBeenCalledWith('channel'))
    expect(screen.getByText('ads')).toBeInTheDocument()
    expect(screen.queryByText('US')).not.toBeInTheDocument()
  })
})
