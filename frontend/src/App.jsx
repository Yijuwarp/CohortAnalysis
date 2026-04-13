import { useCallback, useEffect, useMemo, useRef, useState, Component } from 'react'
import { getRetention, getScope, listCohorts, listEvents, uploadCSV, getColumns } from './api'
import { isCohortSchemaValid } from './utils/cohortUtils'
import Mapping from './components/Mapping'
import FilterData from './components/FilterData'
import RevenueConfig from './components/RevenueConfig'
import CohortPane from './components/CohortPane'
import RetentionTable from './components/RetentionTable'
import UsageTable from './components/UsageTable'
import MonetizationTable from './components/MonetizationTable'
import SearchableSelect from './components/SearchableSelect'
import TopToolbar from './components/TopToolbar'
import FlowPane from './components/FlowPane'
import UserExplorer from './components/UserExplorer'
import PathsPane from './components/PathsPane'
import ExperimentImpactPane from './components/ExperimentImpactPane'
import Login from './components/Login'
import { validateExportSnapshot } from './utils/exportValidator'
import ExportModal from './components/ExportModal'

if (!UserExplorer) {
  console.error('UserExplorer component failed to load from ./components/UserExplorer')
}

const WORKSPACE_STORAGE_VERSION = 2
const LEFT_PANE_WIDTH = 600

const getWorkspaceStorageKey = (userId) => `cohort-analysis-workspace-v2-${userId || 'guest'}`
const getAnalyticsStorageKey = (userId) => `analytics-state-${userId || 'guest'}`

const REASON_MESSAGES = {
  version_mismatch: "Your session is outdated after a recent update.",
  malformed_structure: "The saved data appears to be corrupted.",
  invalid_flow_state: "The session could not be restored.",
  unknown: "An unexpected error occurred while loading your session."
};

const isValidState = (state) => {
  if (!state || typeof state !== "object") return false;
  if (!["empty", "mapping", "workspace"].includes(state.appState)) return false;

  // Validate fields required for specific appState
  if (state.appState === "workspace") {
    if (!state.datasetMeta || typeof state.datasetMeta !== "object") return false;
    if (!Array.isArray(state.columns)) return false;
  }
  
  if (state.appState === "mapping") {
    if (!Array.isArray(state.columns)) return false;
  }

  return true;
};

function readPersistedState(userId) {
  try {
    const key = getWorkspaceStorageKey(userId)
    const raw = localStorage.getItem(key)
    if (!raw) return { status: "EMPTY" }
    
    const parsed = JSON.parse(raw)
    
    if (!parsed || typeof parsed !== 'object') {
       console.warn("Workspace state corrupted (not an object):", { raw });
       return { status: "CORRUPTED", reason: "malformed_structure" }
    }

    if (parsed.version !== WORKSPACE_STORAGE_VERSION) {
      console.warn("Workspace state version mismatch:", { expected: WORKSPACE_STORAGE_VERSION, found: parsed.version });
      return { status: "CORRUPTED", reason: "version_mismatch" }
    }

    if (!isValidState(parsed.state)) {
      console.warn("Workspace state failed contract validation:", { state: parsed.state });
      return { status: "CORRUPTED", reason: "malformed_structure" }
    }

    return { status: "VALID", state: parsed.state }
  } catch (err) {
    console.warn("Workspace state corrupted (parse error):", { err });
    return { status: "CORRUPTED", reason: "malformed_structure" }
  }
}

function readAnalyticsState(userId) {
  try {
    const key = getAnalyticsStorageKey(userId)
    const raw = localStorage.getItem(key)
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    // Lazy validation: if it's not a valid object, just return empty
    if (!parsed || typeof parsed !== 'object') {
      console.warn("Analytics state corrupted, falling back to empty:", { raw });
      return {};
    }
    return parsed
  } catch (err) {
    console.warn("Failed to read persisted analytics state:", err)
    return {}
  }
}

// Global-Aware Reset Dialog
function ResetDialog({ reason, onReset }) {
  const message = REASON_MESSAGES[reason] || REASON_MESSAGES.unknown;
  
  return (
    <div className="reset-dialog-overlay" style={{
      position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
      backgroundColor: 'rgba(255, 255, 255, 0.95)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      zIndex: 9999, padding: '20px'
    }}>
      <div className="card onboarding-card" style={{ maxWidth: '500px', textAlign: 'center' }}>
        <h2 style={{ color: '#1f2937', marginBottom: '12px' }}>We couldn't restore your last session</h2>
        <p style={{ color: '#4b5563', marginBottom: '24px' }}>{message}</p>
        <div style={{ display: 'flex', gap: '12px', justifyContent: 'center' }}>
          <button 
            className="button button-primary"
            onClick={onReset}
            style={{ background: '#ef4444', borderColor: '#ef4444' }}
          >
            Reset Session & Restart
          </button>
          <button 
            className="button"
            onClick={() => window.location.reload()}
          >
            Try Again
          </button>
        </div>
      </div>
    </div>
  );
}

// Simple Class-based Error Boundary for unforeseen logic bugs
export class SimpleErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    console.error("Unforeseen runtime crash:", error, errorInfo);
  }

  handleReset = () => {
    // Note: This global reset clears the current user's state only if we can find it
    const userId = localStorage.getItem('user_id');
    if (userId) {
       localStorage.removeItem(getWorkspaceStorageKey(userId));
       localStorage.removeItem(getAnalyticsStorageKey(userId));
    }
    window.location.reload();
  };

  render() {
    if (this.state.hasError) {
      return (
        <div className="error-boundary-container" style={{ padding: '60px 20px', textAlign: 'center', fontFamily: 'system-ui' }}>
          <div className="card onboarding-card" style={{ maxWidth: '600px', margin: '0 auto' }}>
            <h2 style={{ color: '#ef4444', marginBottom: '16px' }}>An unexpected error occurred</h2>
            <p style={{ color: '#666', marginBottom: '24px' }}>
              {this.state.error?.message || "A critical error stopped the application from rendering."}
            </p>
            <div style={{ display: 'flex', gap: '12px', justifyContent: 'center' }}>
              <button 
                className="button button-primary"
                onClick={this.handleReset}
                style={{ background: '#ef4444', borderColor: '#ef4444' }}
              >
                Reset Application
              </button>
              <button 
                className="button"
                onClick={() => window.location.reload()}
              >
                Retry
              </button>
            </div>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

function AppWorkspace({ userId, onLogout }) {
  const persistedResult = useMemo(() => readPersistedState(userId), [userId])
  const persisted = persistedResult.status === "VALID" ? persistedResult.state : null;

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
  const [activeTab, setActiveTab] = useState(() => {
    const raw = persisted?.activeTab || 'retention'
    const validTabs = ['retention', 'usage', 'monetization', 'paths', 'flow', 'experiment-impact', 'user-explorer']
    return validTabs.includes(raw) ? raw : 'retention'
  })
  const [banner, setBanner] = useState('')
  const [uploading, setUploading] = useState(false)
  const [uploadError, setUploadError] = useState('')
  const [isLeftPaneCollapsed, setIsLeftPaneCollapsed] = useState(false)
  const [isExploreTransitioning, setIsExploreTransitioning] = useState(false)
  const [leftPaneTab, setLeftPaneTab] = useState(persisted?.leftPaneTab || 'filters')
  const [events, setEvents] = useState([])
  const [analyticsState, setAnalyticsState] = useState(() => readAnalyticsState(userId))
  const [scopeVersion, setScopeVersion] = useState(0)
  const [cohorts, setCohorts] = useState([])
  const [cohortsRefreshToken, setCohortsRefreshToken] = useState(0)
  const [exportBuffer, setExportBuffer] = useState([])
  const [appliedFilters, setAppliedFilters] = useState([])
  const [isExportModalOpen, setIsExportModalOpen] = useState(false)

  const activeFilterCount = useMemo(() => appliedFilters.length, [appliedFilters])
  const TAB_KEYS = useMemo(() => ['retention', 'usage', 'monetization', 'paths', 'flow', 'experiment-impact', 'user-explorer'], [])
  
  const [staleTabs, setStaleTabs] = useState(() => TAB_KEYS.reduce((acc, tab) => ({ ...acc, [tab]: false }), {}))
  const [tabRefreshTokens, setTabRefreshTokens] = useState(() => TAB_KEYS.reduce((acc, tab) => ({ ...acc, [tab]: 0 }), {}))
  const [tabReloading, setTabReloading] = useState(() => TAB_KEYS.reduce((acc, tab) => ({ ...acc, [tab]: false }), {}))

  // ---------------------------------------------------------------------------
  // Action Icons
  // ---------------------------------------------------------------------------

  const SearchIcon = () => (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="11" cy="11" r="8"></circle>
      <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
    </svg>
  );

  const SettingsIcon = () => (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="3"></circle>
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"></path>
    </svg>
  );

  const UsersIcon = () => (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path>
      <circle cx="9" cy="7" r="4"></circle>
      <path d="M23 21v-2a4 4 0 0 0-3-3.87"></path>
      <path d="M16 3.13a4 4 0 0 1 0 7.75"></path>
    </svg>
  );

  const ChevronLeft = () => (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="15 18 9 12 15 6"></polyline>
    </svg>
  );

  const ChevronRight = () => (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="9 18 15 12 9 6"></polyline>
    </svg>
  );

  const leftPaneTabs = useMemo(() => [
    { key: 'filters', icon: <SearchIcon />, label: 'Filters' },
    { key: 'settings', icon: <SettingsIcon />, label: 'Analytics Settings' },
    { key: 'cohorts', icon: <UsersIcon />, label: 'Cohorts' },
  ], [])

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

  const triggerCohortRefresh = useCallback(() => {
    setCohortsRefreshToken(prev => prev + 1)
    markTabsStale()
  }, [markTabsStale])

  const refreshTab = useCallback(async (tabKey) => {
    setTabReloading(prev => ({ ...prev, [tabKey]: true }))
    try {
      const [eventsPayload, cohortsPayload, columnsRes] = await Promise.all([
        listEvents(),
        listCohorts(),
        getColumns().catch(() => ({ columns: [] }))
      ])
      setEvents(eventsPayload.events || [])
      const columnsSet = new Set((columnsRes.columns || []).map(c => c.name))
      const enrichedCohorts = (cohortsPayload.cohorts || []).map(c => {
        const schemaValid = isCohortSchemaValid(c, columnsSet)
        return {
          ...c,
          isInvalid: !schemaValid || Number(c.size) === 0,
          isSchemaInvalid: !schemaValid,
        }
      })
      setCohorts(enrichedCohorts)
      
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
      const nextTabState = typeof newState === 'function' ? newState(prevTabState) : newState
      
      if (JSON.stringify(prevTabState) === JSON.stringify(nextTabState)) {
        return prev
      }
      return {
        ...prev,
        [tab]: nextTabState,
      }
    })
  }, [])

  const addToExport = useCallback((snapshot) => {
    try {
      validateExportSnapshot(snapshot)
      if (exportBuffer.length >= 10) {
        setBanner('Export limit reached (10)')
        return
      }
      setExportBuffer(prev => [...prev, snapshot])
      setBanner(`Added to export: ${snapshot.title}`)
    } catch (err) {
      setBanner(`Failed to add to export: ${err.message}`)
    }
  }, [exportBuffer])

  const removeExportItem = useCallback((id) => {
    setExportBuffer(prev => prev.filter(item => item.id !== id))
  }, [])

  const clearExportBuffer = useCallback(() => {
    setExportBuffer([])
    setIsExportModalOpen(false)
  }, [])

  useEffect(() => {
    if (uploading) return

    localStorage.setItem(
      getWorkspaceStorageKey(userId),
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
  }, [appState, columns, detectedTypes, suggestedMappings, datasetMeta, selectedRetentionEvent, globalMaxDay, detectedMaxDay, activeTab, leftPaneTab, uploading, userId])

  useEffect(() => {
    localStorage.setItem(getAnalyticsStorageKey(userId), JSON.stringify(analyticsState))
  }, [analyticsState, userId])

  useEffect(() => {
    if (!banner) return
    const id = setTimeout(() => setBanner(''), 5000)
    return () => clearTimeout(id)
  }, [banner])

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

  useEffect(() => {
    const load = async () => {
      try {
        const [eventsPayload, cohortsPayload, columnsRes] = await Promise.all([
          listEvents(),
          listCohorts(),
          getColumns().catch(() => ({ columns: [] }))
        ])
        setEvents(eventsPayload.events || [])
        const columnsSet = new Set((columnsRes.columns || []).map(c => c.name))
        const enrichedCohorts = (cohortsPayload.cohorts || []).map(c => {
          const schemaValid = isCohortSchemaValid(c, columnsSet)
          return {
            ...c,
            isInvalid: !schemaValid || Number(c.size) === 0,
            isSchemaInvalid: !schemaValid,
          }
        })
        setCohorts(enrichedCohorts)
      } catch {
        setEvents([])
        setCohorts([])
      }
    }
    if (appState === 'workspace') {
      load()
    }
  }, [appState, cohortsRefreshToken])

  const handleHardReset = () => {
    localStorage.removeItem(getWorkspaceStorageKey(userId));
    localStorage.removeItem(getAnalyticsStorageKey(userId));
    window.location.reload();
  };

  const clearPersistedState = () => {
    localStorage.removeItem(getWorkspaceStorageKey(userId))
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
    localStorage.removeItem(getAnalyticsStorageKey(userId))
    setAnalyticsState({})
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
    setCohorts([])

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
    localStorage.removeItem(getWorkspaceStorageKey(userId));

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
	
    setCohorts([])
    triggerCohortRefresh()
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

  const openPaneSection = (key) => {
    setIsLeftPaneCollapsed(false)
    setLeftPaneTab(key)
  }

  if (persistedResult.status === "CORRUPTED") {
    return <ResetDialog reason={persistedResult.reason} onReset={handleHardReset} />;
  }

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
          <p><strong>Example format:</strong></p>
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
            exportCount={exportBuffer.length}
            exportBuffer={exportBuffer}
            onRemoveExportItem={removeExportItem}
            onOpenExportModal={() => setIsExportModalOpen(true)}
            onLogout={onLogout}
          />

          {isExportModalOpen && (
            <ExportModal
              exportBuffer={exportBuffer}
              onClose={() => setIsExportModalOpen(false)}
              onClearAll={clearExportBuffer}
            />
          )}

          {uploadError && <p className="error">{uploadError}</p>}
          {banner && <div className="workspace-banner">{banner}</div>}

          <div className="workspace-body">
            <aside className={`left-pane ${isLeftPaneCollapsed ? 'collapsed' : ''}`} style={{ width: isLeftPaneCollapsed ? 58 : LEFT_PANE_WIDTH }}>
              <div className="left-pane-header">
                {!isLeftPaneCollapsed && (
                  <div className="left-pane-header-expanded">
                    <div className="left-pane-title-row">
                      <div className="left-pane-header-left">
                        <div className="primary-section-icon" title={leftPaneTabs.find(t => t.key === leftPaneTab)?.label}>
                          {leftPaneTabs.find(t => t.key === leftPaneTab)?.icon}
                        </div>
                        <span className="left-pane-section-label">
                          {leftPaneTabs.find(t => t.key === leftPaneTab)?.label}
                        </span>
                      </div>
                      <button 
                        className="left-pane-header-action" 
                        onClick={() => setIsLeftPaneCollapsed(true)}
                        title="Collapse sidebar"
                      >
                        <ChevronLeft />
                      </button>
                    </div>
                  </div>
                )}
                
                {isLeftPaneCollapsed && (
                  <button className="toggle-circle collapsed" onClick={() => setIsLeftPaneCollapsed(false)} title="Expand sidebar">
                    <ChevronRight />
                  </button>
                )}
              </div>

              {isLeftPaneCollapsed ? (
                <div className="icon-rail">
                  {leftPaneTabs.map((tab) => (
                    <button 
                      key={tab.key} 
                      className={`icon-rail-button ${leftPaneTab === tab.key ? 'active' : ''}`}
                      onClick={() => openPaneSection(tab.key)} 
                      title={tab.label}
                    >
                      {tab.icon}
                    </button>
                  ))}
                </div>
              ) : (
                <>
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
                        {tab.icon}
                      </button>
                    ))}
                  </div>
                  {leftPaneTab === 'filters' && (
                    <section className="pane-section pane-section-expanded">
                      <p className="pane-section-hint">Date range • Property filters</p>
                      <FilterData 
                        refreshToken={tabRefreshTokens.retention} 
                        onFiltersApplied={(filters, options = {}) => {
                          setAppliedFilters(filters)
                          if (!options.skipStale) {
                            markTabsStale()
                            triggerCohortRefresh()
                          }
                        }} 
                      />
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
                                options={events}
                                pinnedOptions={[{ label: 'Any Event', value: 'any' }]}
                                value={selectedRetentionEvent}
                                onChange={setSelectedRetentionEvent}
                                placeholder="Select retention event"
                                column="event_name"
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
                      <CohortPane 
                        refreshToken={cohortsRefreshToken}
                        onCohortsChanged={triggerCohortRefresh} 
                        datasetMetadata={datasetMeta}
                      />
                    </section>
                  )}
                </>
              )}
            </aside>

            <section className="analytics-area">
              <div className="analytics-tabs ui-tabs">
                {TAB_KEYS.map(key => {
                  const labels = {
                    retention: 'Retention', usage: 'Usage', monetization: 'Monetization', paths: 'Paths', flow: 'Flows', 'experiment-impact': 'Experiment Impact', 'user-explorer': 'User Explorer'
                  }
                  return (
                    <button key={key} className={activeTab === key ? 'active' : ''} onClick={() => {
                      setActiveTab(key)
                      setStaleTabs(prev => ({ ...prev, [key]: false }))
                    }}>
                      {labels[key]}
                    </button>
                  )
                })}
				</div>

              {staleTabs[activeTab] && (
                <div className="stale-banner" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 16px', background: '#fffbeb', border: '1px solid #fde68a', borderRadius: '8px', marginBottom: '16px', position: 'relative', zIndex: 9 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#b45309', fontWeight: 'bold' }}>
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
                    cohorts={cohorts.filter(c => !c.isInvalid)}
                    appliedFilters={appliedFilters}
                    onAddToExport={addToExport}
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
                     cohorts={cohorts.filter(c => !c.isInvalid)}
                     appliedFilters={appliedFilters}
                     onAddToExport={addToExport}
                   />
                 )}
                 {activeTab === 'monetization' && (
                   <MonetizationTable
                     refreshToken={tabRefreshTokens.monetization}
                     maxDay={globalMaxDay}
                     retentionEvent={selectedRetentionEvent}
                     state={analyticsState.monetization}
                     setState={(s) => updateAnalyticsState('monetization', s)}
                     cohorts={cohorts.filter(c => !c.isInvalid)}
                     appliedFilters={appliedFilters}
                     onAddToExport={addToExport}
                   />
                 )}
 
                 {activeTab === 'paths' && (
                   <PathsPane
                     refreshToken={tabRefreshTokens.paths}
                     events={events}
                     state={analyticsState.paths}
                     setState={(s) => updateAnalyticsState('paths', s)}
                     onRefreshCohorts={triggerCohortRefresh}
                     appliedFilters={appliedFilters}
                     onAddToExport={addToExport}
                   />
                 )}
                 {activeTab === 'flow' && (
                   <FlowPane
                     refreshToken={tabRefreshTokens.flow}
                     state={analyticsState.flows}
                     setState={(s) => updateAnalyticsState('flows', s)}
                     appliedFilters={appliedFilters}
                     onAddToExport={addToExport}
                   />
                 )}
                 {activeTab === 'experiment-impact' && (
                    <ExperimentImpactPane
                      refreshToken={tabRefreshTokens['experiment-impact']}
                      cohorts={cohorts.filter(c => !c.isInvalid)}
                      globalMaxDay={globalMaxDay}
                      appliedFilters={appliedFilters}
                      retentionEvent={selectedRetentionEvent}
                      state={analyticsState['experiment-impact']}
                      setState={(s) => updateAnalyticsState('experiment-impact', s)}
                    />
                  )}
                 {activeTab === 'user-explorer' && (
                   <UserExplorer
                      state={analyticsState['user-explorer']}
                      setState={(s) => updateAnalyticsState('user-explorer', s)}
                      appliedFilters={appliedFilters}
                      onAddToExport={addToExport}
                   />
                 )}
               </div>
            </section>
          </div>
        </div>
      )}
    </main>
  );
}

export default function App() {
  const [userId, setUserId] = useState(() => {
    const raw = localStorage.getItem('user_id')
    if (raw && /^[a-f0-9]{8}$/.test(raw)) {
      return raw
    }
    localStorage.removeItem('user_id')
    return null
  })

  const handleLogout = () => {
    localStorage.removeItem('user_id');
    setUserId(null);
    window.location.reload();
  };

  if (!userId) {
    return <Login onLogin={setUserId} />;
  }

  return (
    <SimpleErrorBoundary>
      <AppWorkspace key={userId} userId={userId} onLogout={handleLogout} />
    </SimpleErrorBoundary>
  );
}
