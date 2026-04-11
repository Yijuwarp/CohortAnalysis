import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import PathsPane from './PathsPane'

// Mock the API
vi.mock('../api', () => ({
  listPaths: vi.fn(),
  runPaths: vi.fn(),
  createPathsDropOffCohort: vi.fn(),
  createPathsReachedCohort: vi.fn(),
  deletePath: vi.fn(),
}))

// Mock Sub-components to inspect props
vi.mock('./PathsBuilderModal', () => ({
  default: vi.fn(({ editingPath, mode, isOpen }) => (
    isOpen ? (
      <div data-testid="mock-modal">
        <span data-testid="modal-mode">{mode}</span>
        <span data-testid="modal-path-name">{editingPath?.name}</span>
        <span data-testid="modal-path-id">{editingPath?.id || 'no-id'}</span>
      </div>
    ) : null
  ))
}))

import { listPaths as listPathsMock } from '../api'

describe('PathsPane - Copy Path functionality', () => {
  const mockPaths = [
    { 
      id: 1, 
      name: 'Signup Funnel', 
      is_valid: true,
      steps: [{ step_order: 0, groups: [{ event_name: 'view_page' }] }],
      created_at: '2024-01-01',
      results: { some: 'stale data' }
    }
  ]

  beforeEach(() => {
    vi.clearAllMocks()
    listPathsMock.mockResolvedValue(mockPaths)
  })

  it('sanitizes and prefixes name correctly when copying', async () => {
    render(<PathsPane state={{ selectedPathId: 1 }} events={[]} setState={() => {}} />)

    // Wait for paths to load and select the path
    await screen.findByText(/Signup Funnel/)

    // Find and click the Duplicate button
    const copyBtn = screen.getByLabelText(/Duplicate path/i)
    fireEvent.click(copyBtn)

    // Check modal props via our mock
    expect(screen.getByTestId('modal-mode').textContent).toBe('create')
    expect(screen.getByTestId('modal-path-name').textContent).toBe('Signup Funnel (copy)')
    expect(screen.getByTestId('modal-path-id').textContent).toBe('no-id')
  })

  it('prevents duplicate (copy) suffixes', async () => {
    const pathsWithCopy = [
      { 
        id: 2, 
        name: 'Login Funnel (copy)', 
        is_valid: true,
        steps: [{ step_order: 0, groups: [{ event_name: 'login' }] }]
      }
    ]
    listPathsMock.mockResolvedValue(pathsWithCopy)

    render(<PathsPane state={{ selectedPathId: 2 }} events={[]} setState={() => {}} />)

    await screen.findByText(/Login Funnel \(copy\)/)

    const copyBtn = screen.getByLabelText(/Duplicate path/i)
    fireEvent.click(copyBtn)

    // Should NOT be "Login Funnel (copy) (copy)"
    expect(screen.getByTestId('modal-path-name').textContent).toBe('Login Funnel (copy)')
  })

  it('handles empty path names gracefully', async () => {
    const pathsNoName = [
      { id: 3, name: '', is_valid: true, steps: [] }
    ]
    listPathsMock.mockResolvedValue(pathsNoName)

    render(<PathsPane state={{ selectedPathId: 3 }} events={[]} setState={() => {}} />)

    await screen.findByDisplayValue(/— Select Path —/) // Wait for load
    
    // Simulate selection change if not auto-selected
    fireEvent.change(screen.getByRole('combobox'), { target: { value: '3' } })

    const copyBtn = screen.getByLabelText(/Duplicate path/i)
    fireEvent.click(copyBtn)

    expect(screen.getByTestId('modal-path-name').textContent).toBe('(copy)')
  })
})

describe('PathsPane - Dual Path Comparison', () => {
  const mockPaths = [
    { id: 1, name: 'Path A', is_valid: true, steps: [{ step_order: 0, groups: [{ event_name: 'a' }] }] },
    { id: 2, name: 'Path B', is_valid: true, steps: [{ step_order: 0, groups: [{ event_name: 'b' }] }] },
  ]

  beforeEach(() => {
    vi.clearAllMocks()
    listPathsMock.mockResolvedValue(mockPaths)
  })

  it('toggles compare dropdown visibility', async () => {
    render(<PathsPane state={{ selectedPathId: 1 }} events={[]} setState={() => {}} />)
    await screen.findByText('Path A')

    // Initially one selector
    expect(screen.getAllByTestId('paths-selector')).toHaveLength(1)

    // Click Compare toggle
    const compareBtn = screen.getByLabelText(/Compare another path/i)
    fireEvent.click(compareBtn)

    // Should now find two selectors
    expect(screen.getAllByTestId('paths-selector')).toHaveLength(2)

    // Toggle off
    fireEvent.click(compareBtn)
    expect(screen.getAllByTestId('paths-selector')).toHaveLength(1)
  })

  it('filters out selected path from compare dropdown', async () => {
    render(<PathsPane state={{ selectedPathId: 1 }} events={[]} setState={() => {}} />)
    await screen.findByText('Path A')

    const compareBtn = screen.getByLabelText(/Compare another path/i)
    fireEvent.click(compareBtn)

    const selectors = screen.getAllByRole('combobox')
    const compareSelector = selectors[1]

    // Should NOT find Path A in the compare selector
    const options = Array.from(compareSelector.options).map(o => o.text)
    expect(options).toContain('Path B')
    expect(options).not.toContain('Path A')
  })

  it('persists comparePathId via setState', async () => {
    const setState = vi.fn()
    render(<PathsPane state={{ selectedPathId: 1 }} events={[]} setState={setState} />)
    await screen.findByText('Path A')

    const compareBtn = screen.getByLabelText(/Compare another path/i)
    fireEvent.click(compareBtn)

    const selectors = screen.getAllByRole('combobox')
    fireEvent.change(selectors[1], { target: { value: '2' } })

    expect(setState).toHaveBeenCalledWith(expect.objectContaining({
      selectedPathId: 1,
      comparePathId: 2
    }))
  })

  it('adds two payloads to export when comparing', async () => {
    const onAddToExport = vi.fn()
    const mockResultA = {
      path_name: 'Path A',
      steps: ['a'],
      results: [
        { cohort_id: 'all', cohort_name: 'All Users', cohort_size: 100, steps: [{ step: 1, event: 'a', users: 100, conversion_pct: 100 }] }
      ]
    }
    const mockResultB = {
      path_name: 'Path B',
      steps: ['b'],
      results: [
        { cohort_id: 'all', cohort_name: 'All Users', cohort_size: 100, steps: [{ step: 1, event: 'b', users: 100, conversion_pct: 100 }] }
      ]
    }

    render(
      <PathsPane 
        state={{ 
          selectedPathId: 1, 
          comparePathId: 2,
          results: { 1: mockResultA, 2: mockResultB } 
        }} 
        events={[]} 
        setState={() => {}} 
        onAddToExport={onAddToExport} 
      />
    )

    const exportBtn = screen.getByLabelText(/Add to Export/i)
    fireEvent.click(exportBtn)

    // Should be called twice
    expect(onAddToExport).toHaveBeenCalledTimes(2)
    expect(onAddToExport).toHaveBeenNthCalledWith(1, expect.objectContaining({
      title: expect.stringContaining('Path A — Path A')
    }))
    expect(onAddToExport).toHaveBeenNthCalledWith(2, expect.objectContaining({
      title: expect.stringContaining('Path B — Path B')
    }))
  })

  it('clears results when primary path changes', async () => {
    const mockResult = { steps: ['a'], results: [] }
    render(
      <PathsPane 
        state={{ selectedPathId: 1, results: { 1: mockResult } }} 
        events={[]} 
        setState={() => {}} 
      />
    )

    // Change Path A
    const selector = screen.getByRole('combobox')
    fireEvent.change(selector, { target: { value: '2' } })

    // Results should be gone, so no funnel chart
    expect(screen.queryByTestId('paths-funnel-chart')).not.toBeInTheDocument()
  })

  it('does NOT show stale results notice immediately when compare is toggled ON', async () => {
    const mockResult = { 
      path_name: 'Path A',
      steps: ['a'], 
      results: [{ cohort_id: 'all', cohort_name: 'All', cohort_size: 1, steps: [] }] 
    }
    render(
      <PathsPane 
        state={{ selectedPathId: 1, results: { 1: mockResult } }} 
        events={[]} 
        setState={() => {}} 
      />
    )

    const compareBtn = screen.getByLabelText(/Compare another path/i)
    fireEvent.click(compareBtn)

    // Stale notice should NOT be present yet
    expect(screen.queryByText(/Results outdated/i)).not.toBeInTheDocument()

    // Now select Path B
    const selectors = screen.getAllByRole('combobox')
    fireEvent.change(selectors[1], { target: { value: '2' } })

    // NOW it should be stale
    expect(screen.getByText(/Results outdated/i)).toBeInTheDocument()
  })
})


