import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import App from './App'
import * as api from './api'

vi.mock('./api', async () => {
  const actual = await vi.importActual('./api')
  return {
    ...actual,
    uploadCSV: vi.fn(),
    getScope: vi.fn(),
    getRetention: vi.fn(),
    listEvents: vi.fn(),
  }
})

vi.mock('./components/Mapping', () => ({
  default: ({ onMappingComplete, datasetName, onUploadNewCSV, uploading }) => (
    <div>
      <div>Mock Mapping UI</div>
      <div>Dataset: {datasetName}</div>
      <button onClick={onUploadNewCSV} disabled={uploading}>Upload New CSV</button>
      <button onClick={onMappingComplete}>Confirm Mapping</button>
    </div>
  ),
}))

vi.mock('./components/FilterData', () => ({ default: () => <div>Mock Filters</div> }))
vi.mock('./components/RevenueConfig', () => ({ default: () => <div>Mock Revenue Config</div> }))
vi.mock('./components/CohortForm', () => ({ default: () => <div>Mock Cohorts</div> }))
vi.mock('./components/RetentionTable', () => ({ default: () => <div>Mock Retention Table</div> }))
vi.mock('./components/UsageTable', () => ({ default: () => <div>Mock Usage Table</div> }))
vi.mock('./components/MonetizationTable', () => ({ default: () => <div>Mock Monetization Table</div> }))
vi.mock('./components/SearchableSelect', () => ({ default: () => <div>Mock Searchable Select</div> }))

describe('App onboarding and workspace flow', () => {
  beforeEach(() => {
    localStorage.clear()
    vi.clearAllMocks()
    api.getScope.mockResolvedValue({ total_rows: 100, total_events: 140, filtered_rows: 100 })
    api.getRetention.mockResolvedValue({ retention_table: [{ cohort_name: 'All Users', size: 42 }] })
    api.listEvents.mockResolvedValue({ events: ['signup'] })
  })

  it('shows onboarding in empty state', () => {
    render(<App />)
    expect(screen.getByText('Cohort Analysis')).toBeInTheDocument()
    expect(screen.getByText('Upload CSV')).toBeInTheDocument()
  })

  it('upload opens mapping screen', async () => {
    api.uploadCSV.mockResolvedValue({ rows_imported: 2, skipped_rows: 0, columns: ['a'], detected_types: {}, mapping_suggestions: null, total_events: 3 })
    render(<App />)

    const input = screen.getByTestId('csv-upload-input')
    fireEvent.change(input, { target: { files: [new File(['a,b\n1,2'], 'events.csv', { type: 'text/csv' })] } })

    expect(await screen.findByText('Mock Mapping UI')).toBeInTheDocument()
  })

  it('keeps analytics hidden until mapping is confirmed', async () => {
    api.uploadCSV.mockResolvedValue({ rows_imported: 2, skipped_rows: 0, columns: ['a'], detected_types: {}, mapping_suggestions: null, total_events: 2 })
    render(<App />)

    fireEvent.change(screen.getByTestId('csv-upload-input'), {
      target: { files: [new File(['a,b\n1,2'], 'events.csv', { type: 'text/csv' })] },
    })

    await screen.findByText('Mock Mapping UI')
    expect(screen.queryByText('Retention')).not.toBeInTheDocument()

    fireEvent.click(screen.getByText('Confirm Mapping'))
    expect(await screen.findByText('Retention')).toBeInTheDocument()
  })

  it('restores workspace from localStorage', async () => {
    localStorage.setItem('cohort-analysis-workspace-v2', JSON.stringify({
      version: 2,
      state: {
        appState: 'workspace',
        columns: ['x'],
        detectedTypes: {},
        suggestedMappings: null,
        datasetMeta: { filename: 'saved.csv', rows: 5, skippedRows: 3, users: 2, events: 10 },
        selectedRetentionEvent: 'any',
        globalMaxDay: 7,
        activeTab: 'retention',
        leftPaneTab: 'filters',
      },
    }))

    render(<App />)

    expect(await screen.findByText(/Dataset: saved.csv/)).toBeInTheDocument()
    expect(await screen.findByText(/Skipped: 3/)).toBeInTheDocument()
    expect(screen.getByText('Retention')).toBeInTheDocument()
  })


  it('allows replacing dataset from mapping view', async () => {
    localStorage.setItem('cohort-analysis-workspace-v2', JSON.stringify({
      version: 2,
      state: {
        appState: 'mapping',
        columns: ['old_col'],
        detectedTypes: {},
        suggestedMappings: null,
        datasetMeta: { filename: 'old.csv', rows: 1, users: 0, events: 1 },
        selectedRetentionEvent: 'any',
        globalMaxDay: 7,
        activeTab: 'retention',
        leftPaneTab: 'filters',
      },
    }))

    api.uploadCSV.mockResolvedValueOnce({ rows_imported: 4, skipped_rows: 1, columns: ['new_col'], detected_types: {}, mapping_suggestions: null, total_events: 4 })
    render(<App />)

    expect(await screen.findByText('Dataset: old.csv')).toBeInTheDocument()

    fireEvent.change(screen.getByTestId('csv-upload-input'), {
      target: { files: [new File(['a,b\n1,2'], 'new.csv', { type: 'text/csv' })] },
    })

    await waitFor(() => expect(api.uploadCSV).toHaveBeenCalledTimes(1))
    expect(await screen.findByText('Dataset: new.csv')).toBeInTheDocument()
    expect(screen.getByText('Mock Mapping UI')).toBeInTheDocument()
  })
  it('clears persisted state only after successful replacement upload', async () => {
    localStorage.setItem('cohort-analysis-workspace-v2', JSON.stringify({ version: 2, state: { appState: 'workspace' } }))

    api.uploadCSV.mockRejectedValueOnce(new Error('upload failed'))
    const { rerender } = render(<App />)
    const input = screen.getByTestId('csv-upload-input')
    fireEvent.change(input, { target: { files: [new File(['a'], 'events.csv', { type: 'text/csv' })] } })

    await waitFor(() => expect(api.uploadCSV).toHaveBeenCalled())
    expect(localStorage.getItem('cohort-analysis-workspace-v2')).not.toBeNull()

    api.uploadCSV.mockResolvedValueOnce({ rows_imported: 2, skipped_rows: 0, columns: ['a'], detected_types: {}, mapping_suggestions: null, total_events: 2 })
    rerender(<App />)
    fireEvent.change(screen.getByTestId('csv-upload-input'), { target: { files: [new File(['a'], 'events.csv', { type: 'text/csv' })] } })

    await screen.findByText('Mock Mapping UI')
    const saved = JSON.parse(localStorage.getItem('cohort-analysis-workspace-v2'))
    expect(saved.version).toBe(2)
    expect(saved.state.appState).toBe('mapping')
  })
})
