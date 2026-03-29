import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { getRetention, getScope, listCohorts, listEvents, uploadCSV } from './api'
import Mapping from './components/Mapping'
import FilterData from './components/FilterData'
import RevenueConfig from './components/RevenueConfig'
import CohortPane from './components/CohortPane'
import RetentionTable from './components/RetentionTable'
import UsageTable from './components/UsageTable'
import MonetizationTable from './components/MonetizationTable'
import FunnelPane from './components/FunnelPane'
import SearchableSelect from './components/SearchableSelect'
import TopToolbar from './components/TopToolbar'
import FlowPane from './components/FlowPane'
import UserExplorer from './components/UserExplorer'
import PathsPane from './components/PathsPane'

if (!UserExplorer) {
  console.error('UserExplorer component failed to load from ./components/UserExplorer')
}

const WORKSPACE_STORAGE_KEY = 'cohort-analysis-workspace-v2'
const ANALYTICS_STORAGE_KEY = 'analytics-state'
const WORKSPACE_STORAGE_VERSION = 2
const LEFT_PANE_WIDTH = 600

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

function readAnalyticsState() {
  try {
    const raw = localStorage.getItem(ANALYTICS_STORAGE_KEY)
    return raw ? JSON.parse(raw) : {}
  } catch {
    return {}
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
  const [detectedMaxDay, setDetectedMaxDay] = useState(persisted?.detectedMaxDay || null)
  const [activeTab, setActiveTab] = useState(persisted?.activeTab || 'retention')
  const [banner, setBanner] = useState('')
  const [uploading, setUploading] = useState(false)
  const [uploadError, setUploadError] = useState('')
  const [isLeftPaneCollapsed, setIsLeftPaneCollapsed] = useState(false)
  const [isExploreTransitioning, setIsExploreTransitioning] = useState(false)
  const [leftPaneTab, setLeftPaneTab] = useState(persisted?.leftPaneTab || 'filters')
  const [events, setEvents] = useState([])
  const [activeFilterCount, setActiveFilterCount] = useState(0)
  const [analyticsState, setAnalyticsState] = useState(() => readAnalyticsState())
  const [scopeVersion, setScopeVersion] = useState(0)
  const [cohorts, setCohorts] = useState([])

  const TAB_KEYS = useMemo(() => ['retention', 'usage', 'monetization', 'paths', 'funnels', 'flow', 'user-explorer'], [])
  const [staleTabs, setStaleTabs] = useState(() => TAB_KEYS.reduce((acc, tab) => ({ ...acc, [tab]: false }), {}))
  const [tabRefreshTokens, setTabRefreshTokens] = useState(() => TAB_KEYS.reduce((acc, tab) => ({ ...acc, [tab]: 0 }), {}))
  const [tabReloading, setTabReloading] = useState(() => TAB_KEYS.reduce((acc, tab) => ({ ...acc, [tab]: false }), {}))

  const markTabsStale = useCallback((tabsObj) => {
    if (tabsObj) {
      setStaleTabs((prev) => ({ ...prev, ...tabsObj }))
    } else {
      setStaleTabs((prev) => {
        const next = { ...prev }
        TAB_KEYS.forEach(k => next[k] = true)
        return next
      })
    }
  }, [TAB_KEYS])

  const refreshTab = useCallback(async (tabKey) => {
    setTabReloading(prev => ({ ...prev, [tabKey]: true }))
    try {
      const [eventsPayload, cohortsPayload] = await Promise.all([
        listEvents(),
        listCohorts()
      ])
      setEvents(eventsPayload.events || [])
      setCohorts(cohortsPayload.cohorts || [])
      
      setStaleTabs(prev => ({ ...prev, [tabKey]: false }))
      setTabRefreshTokens(prev => ({ ...prev, [tabKey]: prev[tabKey] + 1 }))
      setScopeVersion(prev => prev + 1)
    } catch {
      // ignore
    } finally {
      setTabReloading(prev => ({ ...prev, [tabKey]: false }))
    }
  }, [])

  const updateAnalyticsState = useCallback((tab, newState) => {
    setAnalyticsState((prev) => {
      const prevTabState = prev[tab]
      if (JSON.stringify(prevTabState) === JSON.stringify(newState)) {
        return prev
      }
      return {
        ...prev,
        [tab]: newState,
      }
    })
  }, [])

  useEffect(() => {
    if (uploading) return

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
          detectedMaxDay,
          activeTab,
          leftPaneTab,
        },
      })
    )
  }, [appState, columns, detectedTypes, suggestedMappings, datasetMeta, selectedRetentionEvent, globalMaxDay, detectedMaxDay, activeTab, leftPaneTab, uploading])

  useEffect(() => {
    localStorage.setItem(ANALYTICS_STORAGE_KEY, JSON.stringify(analyticsState))
  }, [analyticsState])


  useEffect(() => {
    if (!banner) return
    const id = setTimeout(() => setBanner(''), 5000)
    return () => clearTimeout(id)
  }, [banner])

  const refreshRetention = () => {
    setRetentionRefreshToken((current) => current + 1)
    setScopeVersion((current) => current + 1)
  }

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
  }, [refreshDatasetInfo, tabRefreshTokens])

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

    clearPersistedState()
    setAppState('empty')
    setColumns([])
    setDetectedTypes({})
    setSuggestedMappings(null)
    setDatasetMeta(null)
    setSelectedRetentionEvent('any')
    setGlobalMaxDay(7)
    setDetectedMaxDay(null)
    setActiveTab('retention')
    setLeftPaneTab('filters')

    try {
      const data = await uploadCSV(file)
      setColumns(data.columns || [])
      setDetectedTypes(data.detected_types || {})
      setSuggestedMappings(data.mapping_suggestions || null)
      setDatasetMeta({
        filename: file.name,
        rows: Number(data.rows_imported || 0),
        skippedRows: Number(data.skipped_rows || 0),
        users: 0,
        events: Number(data.total_events || 0),
      })
      setAppState('mapping')
      setBanner('')
      setLeftPaneTab('filters')
    } catch (err) {
      setUploadError(err.message)
    } finally {
      if (fileInputRef.current) {
        fileInputRef.current.value = ''
      }
      setUploading(false)
    }
  }

  const handleMappingComplete = async (data) => {
    clearPersistedState()

    setDatasetMeta((prev) => ({
      ...prev,
      users: data?.total_users ?? prev?.users ?? 0,
      events: data?.total_events ?? prev?.events ?? 0,
    }))

    setDetectedMaxDay(null)
    setGlobalMaxDay(7)

	try {
	  const retData = await getRetention(365, "any")

	  const maxDay = retData?.max_day

	  if (typeof maxDay === "number" && !Number.isNaN(maxDay)) {
		setDetectedMaxDay(maxDay)
		setGlobalMaxDay(maxDay)
	  }
	} catch {
	  // Best effort
	}
	
    markTabsStale()
    setBanner('Mapping complete. Opening Explore Data...')
    setIsExploreTransitioning(true)
    setTimeout(() => {
      setAppState('workspace')
      setActiveTab('retention')
      setLeftPaneTab('filters')
      setBanner('Dataset ready. Create cohorts and explore retention, usage, and monetization analytics.')
      setIsExploreTransitioning(false)
    }, 650)
  }

  useEffect(() => {
    const load = async () => {
      try {
        const [eventsPayload, cohortsPayload] = await Promise.all([
          listEvents(),
          listCohorts()
        ])
        setEvents(eventsPayload.events || [])
        setCohorts(cohortsPayload.cohorts || [])
      } catch {
        setEvents([])
        setCohorts([])
      }
    }
    if (appState === 'workspace') {
      load()
    }
  }, [appState]) // global load on workspace entry

  const openPaneSection = (key) => {
    setIsLeftPaneCollapsed(false)
    setLeftPaneTab(key)
  }

  const leftPaneTabs = useMemo(() => [
    { key: 'filters', icon: '🔎', label: 'Filters' },
    { key: 'settings', icon: '⚙', label: 'Analytics Settings' },
    { key: 'cohorts', icon: '👥', label: 'Cohorts' },
  ], [])

  return (
    <main className="app-container workspace-root">
      <input
        ref={fileInputRef}
        type="file"
        data-testid="csv-upload-input"
        aria-label="Upload CSV"
        accept=".csv"
        style={{ display: 'none' }}
        onChange={(e) => handleUploadFile(e.target.files?.[0])}
      />
      {appState === 'empty' && (
        <section className="card onboarding-card">
          <h1>Cohort Analysis</h1>
          <p>Analyze retention, usage, and monetization from event datasets.</p>
          <p><strong>Required fields:</strong> user_id, event_name, event_time</p>
          <p><strong>Optional fields:</strong> event_count, revenue</p>
          <p>Your dataset can include any number of additional fields such as country, device, version, campaign, etc. These fields can later be used for filtering and cohort definitions.</p>
          <pre className="sample-dataset">user_id,event_name,event_time,country,device,version\nu123,signup,2024-01-01 10:00:00,US,ios,3.9.1</pre>
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
            <span className="active">Map Columns</span>
            <span>Explore Data</span>
          </div>
          <Mapping
            columns={columns}
            detectedTypes={detectedTypes}
            suggestedMappings={suggestedMappings}
            datasetName={datasetMeta?.filename || 'Unknown'}
            onUploadNewCSV={() => fileInputRef.current?.click()}
            uploading={uploading}
            onMappingComplete={handleMappingComplete}
          />
          {uploadError && <p className="error">{uploadError}</p>}
        </>
      )}

      {appState === 'workspace' && (
        <div className={`workspace-layout ${isExploreTransitioning ? 'workspace-enter' : ''}`}>
          <TopToolbar
            hasDataset={Boolean(datasetMeta?.filename)}
            datasetMeta={datasetMeta}
            uploading={uploading}
            onUploadClick={() => fileInputRef.current?.click()}
            onRemapColumns={() => setAppState('mapping')}
            onOpenRevenueConfig={() => openPaneSection('settings')}
            onOpenCreateCohort={() => openPaneSection('cohorts')}
            onToggleFilters={() => openPaneSection('filters')}
            activeFilterCount={activeFilterCount}
            globalMaxDay={globalMaxDay}
            setGlobalMaxDay={setGlobalMaxDay}
          />

          {uploadError && <p className="error">{uploadError}</p>}
          {banner && <div className="workspace-banner">{banner}</div>}

          <div className="workspace-body">
            <aside className={`left-pane ${isLeftPaneCollapsed ? 'collapsed' : ''}`} style={{ width: isLeftPaneCollapsed ? 58 : LEFT_PANE_WIDTH }}>
              <div className="left-pane-header">
                {!isLeftPaneCollapsed && (
                  <div className="left-pane-tabbar">
                    {leftPaneTabs.map((tab) => (
                      <button
                        key={tab.key}
                        type="button"
                        className={`left-pane-tab ${leftPaneTab === tab.key ? 'active' : ''}`}
                        role="tab"
                        aria-selected={leftPaneTab === tab.key}
                        aria-label={tab.label}
                        onClick={() => setLeftPaneTab(tab.key)}
                        title={tab.label}
                      >
                        <span>{tab.icon}</span>
                      </button>
                    ))}
                  </div>
                )}
                <button className={`toggle-circle ${isLeftPaneCollapsed ? 'collapsed' : ''}`} onClick={() => setIsLeftPaneCollapsed((prev) => !prev)}>
                  <span className="triangle-icon">◂</span>
                </button>
              </div>

              {isLeftPaneCollapsed ? (
                <div className="icon-rail">
                  {leftPaneTabs.map((tab) => (
                    <button key={tab.key} onClick={() => openPaneSection(tab.key)} title={tab.label}>{tab.icon}</button>
                  ))}
                </div>
              ) : (
                <>
                  {leftPaneTab === 'filters' && (
                    <section className="pane-section pane-section-expanded">
                      <p className="pane-section-hint">Date range • Property filters</p>
                      <FilterData refreshToken={tabRefreshTokens.retention} onFiltersApplied={() => markTabsStale()} onActiveFilterCountChange={setActiveFilterCount} />
                    </section>
                  )}
                  {leftPaneTab === 'settings' && (
                    <section className="pane-section pane-section-expanded">
                      <p className="pane-section-hint">Max day • Retention event • Revenue configuration</p>
                      <div className="ui-section">
                          <div className="card ui-card">
                            <h4>Max Analysis Day</h4>
                            <div className="settings-control-body">
                              <input type="number" min="0" value={globalMaxDay} onChange={(e) => setGlobalMaxDay(Number(e.target.value))} />
                            </div>
                          </div>

                          <div className="card ui-card">
                            <h4>Retention Event</h4>
                            <div className="settings-control-body">
                              <SearchableSelect
                                options={[{ label: 'Any Event', value: 'any' }, ...events]}
                                value={selectedRetentionEvent}
                                onChange={setSelectedRetentionEvent}
                                placeholder="Select retention event"
                              />
                            </div>
                          </div>

                          <div className="card ui-card">
                            <h4>Revenue Configuration</h4>
                            <RevenueConfig refreshToken={tabRefreshTokens.retention} onUpdated={() => markTabsStale()} />
                          </div>
                        </div>
                    </section>
                  )}
                  {leftPaneTab === 'cohorts' && (
                    <section className="pane-section pane-section-expanded">
                      <p className="pane-section-hint">Create and manage cohorts</p>
                      <CohortPane onCohortsChanged={() => markTabsStale()} />
                    </section>
                  )}
                </>
              )}
            </aside>

            <section className="analytics-area">
              <div className="analytics-tabs ui-tabs">
                {TAB_KEYS.map(key => {
                  const labels = {
                    retention: 'Retention', usage: 'Usage', monetization: 'Monetization', funnels: 'Funnels', paths: 'Paths', flow: 'Flows', 'user-explorer': 'User Explorer'
                  }
                  return (
                    <button key={key} className={activeTab === key ? 'active' : ''} onClick={() => {
                      setActiveTab(key)
                      setStaleTabs(prev => ({ ...prev, [key]: false }))
                    }}>
                      {labels[key]} {staleTabs[key] && <span style={{ marginLeft: 6, color: '#d97706', fontWeight: 'bold' }}>⚠</span>}
                    </button>
                  )
                })}
              </div>

              {staleTabs[activeTab] && (
                <div className="stale-banner" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 16px', background: '#fffbeb', border: '1px solid #fde68a', borderRadius: '8px', marginBottom: '16px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#b45309', fontWeight: 'bold' }}>
                    <span style={{ fontSize: '18px' }}>⚠</span>
                    <span>Data is stale</span>
                  </div>
                  <button 
                    className="button button-primary" 
                    onClick={() => refreshTab(activeTab)}
                    disabled={tabReloading[activeTab]}
                    style={{ background: '#10b981', borderColor: '#10b981', color: 'white', fontWeight: 'bold' }}
                    title="Recompute analytics after changes"
                  >
                    {tabReloading[activeTab] ? 'Reloading...' : 'Reload'}
                  </button>
                </div>
              )}

              <div style={{ opacity: staleTabs[activeTab] ? 0.95 : 1, position: 'relative', flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
                
                {activeTab === 'retention' && (
                  <RetentionTable
                    refreshToken={tabRefreshTokens.retention}
                    retentionEvent={selectedRetentionEvent}
                    onRetentionEventChange={setSelectedRetentionEvent}
                    maxDay={globalMaxDay}
                    setMaxDay={setGlobalMaxDay}
                    showGlobalControls={false}
                    state={analyticsState.retention}
                    setState={(s) => updateAnalyticsState('retention', s)}
                    cohorts={cohorts}
                  />
                )}
                {activeTab === 'usage' && (
                  <UsageTable
                    refreshToken={tabRefreshTokens.usage}
                    retentionEvent={selectedRetentionEvent}
                    maxDay={globalMaxDay}
                    state={analyticsState.usage}
                    setState={(s) => updateAnalyticsState('usage', s)}
                    scopeVersion={scopeVersion}
                    cohorts={cohorts}
                  />
                )}
                {activeTab === 'monetization' && (
                  <MonetizationTable
                    refreshToken={tabRefreshTokens.monetization}
                    maxDay={globalMaxDay}
                    retentionEvent={selectedRetentionEvent}
                    state={analyticsState.monetization}
                    setState={(s) => updateAnalyticsState('monetization', s)}
                    cohorts={cohorts}
                  />
                )}
                {activeTab === 'funnels' && (
                  <FunnelPane
                    refreshToken={tabRefreshTokens.funnels}
                    events={events}
                    state={analyticsState.funnels}
                    setState={(s) => updateAnalyticsState('funnels', s)}
                  />
                )}
                {activeTab === 'paths' && (
                  <PathsPane
                    refreshToken={tabRefreshTokens.paths}
                    events={events}
                    state={analyticsState.paths}
                    setState={(s) => updateAnalyticsState('paths', s)}
                    onRefreshCohorts={() => markTabsStale()}
                  />
                )}
                {activeTab === 'flow' && (
                  <FlowPane
                    refreshToken={tabRefreshTokens.flow}
                    state={analyticsState.flows}
                    setState={(s) => updateAnalyticsState('flows', s)}
                  />
                )}
                {activeTab === 'user-explorer' && (
                  <UserExplorer
                     state={analyticsState['user-explorer']}
                     setState={(s) => updateAnalyticsState('user-explorer', s)}
                  />
                )}
              </div>
            </section>
          </div>
        </div>
      )}
    </main>
  )
}
