import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { getRetention, getScope, listEvents, uploadCSV } from './api'
import Mapping from './components/Mapping'
import FilterData from './components/FilterData'
import RevenueConfig from './components/RevenueConfig'
import CohortForm from './components/CohortForm'
import RetentionTable from './components/RetentionTable'
import UsageTable from './components/UsageTable'
import MonetizationTable from './components/MonetizationTable'
import SearchableSelect from './components/SearchableSelect'

const WORKSPACE_STORAGE_KEY = 'cohort-analysis-workspace-v2'
const WORKSPACE_STORAGE_VERSION = 2
const LEFT_PANE_WIDTH = 320

function readPersistedState() {
  try {
    const raw = localStorage.getItem(WORKSPACE_STORAGE_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw)
    if (!parsed || parsed.version !== WORKSPACE_STORAGE_VERSION || typeof parsed.state !== 'object') {
      return null
    }
    return parsed.state
  } catch {
    return null
  }
}

export default function App() {
  const persisted = useMemo(() => readPersistedState(), [])
  const fileInputRef = useRef(null)
  const [appState, setAppState] = useState(persisted?.appState || 'empty')
  const [columns, setColumns] = useState(persisted?.columns || [])
  const [detectedTypes, setDetectedTypes] = useState(persisted?.detectedTypes || {})
  const [suggestedMappings, setSuggestedMappings] = useState(persisted?.suggestedMappings || null)
  const [datasetMeta, setDatasetMeta] = useState(persisted?.datasetMeta || null)
  const [retentionRefreshToken, setRetentionRefreshToken] = useState(0)
  const [selectedRetentionEvent, setSelectedRetentionEvent] = useState(persisted?.selectedRetentionEvent || 'any')
  const [globalMaxDay, setGlobalMaxDay] = useState(persisted?.globalMaxDay || 7)
  const [activeTab, setActiveTab] = useState(persisted?.activeTab || 'retention')
  const [banner, setBanner] = useState('')
  const [uploading, setUploading] = useState(false)
  const [uploadError, setUploadError] = useState('')
  const [showReplaceWarning, setShowReplaceWarning] = useState(false)
  const [isTopBarCollapsed, setIsTopBarCollapsed] = useState(false)
  const [isLeftPaneCollapsed, setIsLeftPaneCollapsed] = useState(false)
  const [sections, setSections] = useState(persisted?.sections || { filters: true, settings: true, cohorts: true })
  const [events, setEvents] = useState([])

  useEffect(() => {
    localStorage.setItem(
      WORKSPACE_STORAGE_KEY,
      JSON.stringify({
        version: WORKSPACE_STORAGE_VERSION,
        state: {
          appState,
          columns,
          detectedTypes,
          suggestedMappings,
          datasetMeta,
          selectedRetentionEvent,
          globalMaxDay,
          activeTab,
          sections,
        },
      })
    )
  }, [appState, columns, detectedTypes, suggestedMappings, datasetMeta, selectedRetentionEvent, globalMaxDay, activeTab, sections])

  useEffect(() => {
    if (!banner) return
    const id = setTimeout(() => setBanner(''), 5000)
    return () => clearTimeout(id)
  }, [banner])

  const refreshRetention = () => setRetentionRefreshToken((current) => current + 1)

  const refreshDatasetInfo = useCallback(async () => {
    if (appState !== 'workspace') return
    try {
      const [scope, retention] = await Promise.all([getScope(), getRetention(0, 'any')])
      const allUsers = (retention.retention_table || []).find((row) => row.cohort_name === 'All Users')
      setDatasetMeta((prev) => ({
        ...(prev || {}),
        rows: Number(scope.total_rows || prev?.rows || 0),
        events: Number(scope.total_events || prev?.events || 0),
        users: Number(allUsers?.size || prev?.users || 0),
      }))
    } catch {
      // best effort only
    }
  }, [appState])

  useEffect(() => {
    refreshDatasetInfo()
  }, [refreshDatasetInfo, retentionRefreshToken])

  const clearPersistedState = () => {
    localStorage.removeItem(WORKSPACE_STORAGE_KEY)
  }

  const handleUploadFile = async (file) => {
    if (!file) return
    if (!file.name.toLowerCase().endsWith('.csv')) {
      setUploadError('Only CSV files are supported.')
      return
    }

    setUploading(true)
    setUploadError('')
    try {
      const data = await uploadCSV(file)
      clearPersistedState()
      setColumns(data.columns || [])
      setDetectedTypes(data.detected_types || {})
      setSuggestedMappings(data.mapping_suggestions || null)
      setDatasetMeta({
        filename: file.name,
        rows: Number(data.rows_imported || 0),
        users: 0,
        events: Number(data.total_events || 0),
      })
      setAppState('mapping')
      setBanner('')
      setShowReplaceWarning(false)
      setSections({ filters: true, settings: true, cohorts: true })
    } catch (err) {
      setUploadError(err.message)
    } finally {
      if (fileInputRef.current) {
        fileInputRef.current.value = ''
      }
      setUploading(false)
    }
  }

  const handleMappingComplete = () => {
    clearPersistedState()
    refreshRetention()
    setAppState('workspace')
    setActiveTab('retention')
    setSections({ filters: true, settings: true, cohorts: true })
    setBanner('Dataset ready. Create cohorts and explore retention, usage, and monetization analytics.')
  }

  useEffect(() => {
    const load = async () => {
      try {
        const payload = await listEvents()
        setEvents(payload.events || [])
      } catch {
        setEvents([])
      }
    }
    if (appState === 'workspace') {
      load()
    }
  }, [appState, retentionRefreshToken])

  const openPaneSection = (key) => {
    setIsLeftPaneCollapsed(false)
    setSections({ filters: key === 'filters', settings: key === 'settings', cohorts: key === 'cohorts' })
  }

  return (
    <main className="app-container workspace-root">
      {appState === 'empty' && (
        <section className="card onboarding-card">
          <h1>Cohort Analysis</h1>
          <p>Analyze retention, usage, and monetization from event datasets.</p>
          <p><strong>Required fields:</strong> user_id, event_name, event_time</p>
          <p><strong>Optional fields:</strong> event_count, revenue</p>
          <p>Your dataset can include any number of additional fields such as country, device, version, campaign, etc. These fields can later be used for filtering and cohort definitions.</p>
          <pre className="sample-dataset">user_id,event_name,event_time,country,device,version\nu123,signup,2024-01-01 10:00:00,US,ios,3.9.1</pre>
          <input
            ref={fileInputRef}
            type="file"
            data-testid="csv-upload-input-onboarding"
            aria-label="Upload CSV"
            accept=".csv"
            style={{ display: 'none' }}
            onChange={(e) => handleUploadFile(e.target.files?.[0])}
          />
          <button className="button button-primary" onClick={() => fileInputRef.current?.click()} disabled={uploading}>
            {uploading ? 'Uploading...' : 'Upload CSV'}
          </button>
          {uploadError && <p className="error">{uploadError}</p>}
        </section>
      )}

      {appState === 'mapping' && (
        <>
          <div className="mapping-progress-overlay">
            <span>✓ Upload Dataset</span>
            <span className="active">2 Map Columns</span>
            <span>3 Explore Data</span>
          </div>
          <Mapping
            columns={columns}
            detectedTypes={detectedTypes}
            suggestedMappings={suggestedMappings}
            onMappingComplete={handleMappingComplete}
          />
        </>
      )}

      {appState === 'workspace' && (
        <div className="workspace-layout">
          <header className={`top-bar ${isTopBarCollapsed ? 'collapsed' : ''}`}>
            {!isTopBarCollapsed && (
              <div className="inline-controls" style={{ margin: 0 }}>
                <button className="button button-primary" onClick={() => {
                  if (datasetMeta) setShowReplaceWarning(true)
                  fileInputRef.current?.click()
                }}>Upload CSV</button>
                <button className="button button-secondary" onClick={() => setAppState('mapping')}>Remap Columns</button>
                <button className="button button-secondary" onClick={() => openPaneSection('settings')}>Revenue Config</button>
                <button className="button button-secondary" onClick={() => openPaneSection('cohorts')}>Create Cohort</button>
              </div>
            )}
            <button className="collapse-control" onClick={() => setIsTopBarCollapsed((prev) => !prev)}>{isTopBarCollapsed ? 'v' : '^'}</button>
            {showReplaceWarning && <p className="warning-text">Uploading a new dataset will replace the current dataset and reset cohorts, filters, monetization configuration, and analytics results.</p>}
            {uploadError && <p className="error">{uploadError}</p>}
            <input
              ref={fileInputRef}
              type="file"
              data-testid="csv-upload-input-workspace"
              aria-label="Upload CSV"
              accept=".csv"
              style={{ display: 'none' }}
              onChange={(e) => handleUploadFile(e.target.files?.[0])}
            />
          </header>

          {banner && <div className="workspace-banner">{banner}</div>}

          <div className="dataset-row">
            Dataset: {datasetMeta?.filename || 'Unknown'} | Rows: {datasetMeta?.rows || 0} | Users: {datasetMeta?.users || 0} | Events: {datasetMeta?.events || 0}
          </div>

          <div className="workspace-body">
            <aside className={`left-pane ${isLeftPaneCollapsed ? 'collapsed' : ''}`} style={{ width: isLeftPaneCollapsed ? 58 : LEFT_PANE_WIDTH }}>
              <div className="left-pane-header">
                <button className="button button-secondary" onClick={() => setIsLeftPaneCollapsed((prev) => !prev)}>{isLeftPaneCollapsed ? '>' : '<'}</button>
              </div>

              {isLeftPaneCollapsed ? (
                <div className="icon-rail">
                  <button onClick={() => openPaneSection('filters')}>⏳</button>
                  <button onClick={() => openPaneSection('settings')}>⚙</button>
                  <button onClick={() => openPaneSection('cohorts')}>👥</button>
                </div>
              ) : (
                <>
                  <section className="pane-section">
                    <h3 onClick={() => setSections((prev) => ({ ...prev, filters: !prev.filters }))}>Filters</h3>
                    {sections.filters && <FilterData refreshToken={retentionRefreshToken} onFiltersApplied={refreshRetention} />}
                  </section>
                  <section className="pane-section">
                    <h3 onClick={() => setSections((prev) => ({ ...prev, settings: !prev.settings }))}>Analytics Settings</h3>
                    {sections.settings && (
                      <div className="card">
                        <label>
                          Max Analysis Day
                          <input type="number" min="0" value={globalMaxDay} onChange={(e) => setGlobalMaxDay(Number(e.target.value))} />
                        </label>
                        <label>
                          Retention Event
                          <SearchableSelect
                            options={[{ label: 'Any Event', value: 'any' }, ...events]}
                            value={selectedRetentionEvent}
                            onChange={setSelectedRetentionEvent}
                            placeholder="Select retention event"
                          />
                        </label>
                        <RevenueConfig refreshToken={retentionRefreshToken} onUpdated={refreshRetention} />
                      </div>
                    )}
                  </section>
                  <section className="pane-section">
                    <h3 onClick={() => setSections((prev) => ({ ...prev, cohorts: !prev.cohorts }))}>Cohorts</h3>
                    {sections.cohorts && <CohortForm refreshToken={retentionRefreshToken} onCohortsChanged={refreshRetention} />}
                  </section>
                </>
              )}
            </aside>

            <section className="analytics-area">
              <div className="analytics-tabs">
                <button className={activeTab === 'retention' ? 'active' : ''} onClick={() => setActiveTab('retention')}>Retention</button>
                <button className={activeTab === 'usage' ? 'active' : ''} onClick={() => setActiveTab('usage')}>Usage</button>
                <button className={activeTab === 'monetization' ? 'active' : ''} onClick={() => setActiveTab('monetization')}>Monetization</button>
              </div>
              {activeTab === 'retention' && (
                <RetentionTable
                  refreshToken={retentionRefreshToken}
                  retentionEvent={selectedRetentionEvent}
                  onRetentionEventChange={setSelectedRetentionEvent}
                  maxDay={globalMaxDay}
                  setMaxDay={setGlobalMaxDay}
                  showGlobalControls={false}
                />
              )}
              {activeTab === 'usage' && (
                <UsageTable
                  refreshToken={retentionRefreshToken}
                  retentionEvent={selectedRetentionEvent}
                  maxDay={globalMaxDay}
                />
              )}
              {activeTab === 'monetization' && (
                <MonetizationTable
                  refreshToken={retentionRefreshToken}
                  maxDay={globalMaxDay}
                />
              )}
            </section>
          </div>
        </div>
      )}
    </main>
  )
}
