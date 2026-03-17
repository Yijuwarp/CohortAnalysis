import { useCallback, useEffect, useRef, useState } from 'react'
import { compareCohorts, listCohorts } from '../api'
import { formatDynamic } from '../utils/formatters'

/**
 * Metric definitions per tab.
 * Each entry: { value: backendMetric, label: display label, requiresEvent: bool }
 */
const METRIC_DEFS = {
  retention: [
    { value: 'retention_rate', label: 'Retention %', requiresEvent: false },
  ],
  usage_volume: [
    { value: 'per_installed_user', label: 'Per Installed User', requiresEvent: true },
    { value: 'cumulative_per_installed_user', label: 'Cumulative Per Installed User', requiresEvent: true },
    { value: 'per_retained_user', label: 'Per Retained User', requiresEvent: true },
    { value: 'per_event_firer', label: 'Per Event Firer', requiresEvent: true },
  ],
  usage_users: [
    { value: 'unique_users_percent', label: 'Unique Users %', requiresEvent: true },
    { value: 'unique_users_cumulative_percent', label: 'Cumulative Unique Users %', requiresEvent: true },
  ],
  monetization: [
    { value: 'revenue_per_acquired_user', label: 'Revenue per Acquired User', requiresEvent: false },
    { value: 'cumulative_revenue_per_acquired_user', label: 'Cumulative Revenue per Acquired User', requiresEvent: false },
    { value: 'revenue_per_retained_user', label: 'Revenue per Retained User', requiresEvent: false },
  ],
}

function getMetricsForTab(tab) {
  if (tab === 'retention') return METRIC_DEFS.retention
  if (tab === 'usage') return [...METRIC_DEFS.usage_volume, ...METRIC_DEFS.usage_users]
  if (tab === 'monetization') return METRIC_DEFS.monetization
  return []
}

function getBackendTab(tab) {
  if (tab === 'retention') return 'retention'
  if (tab === 'usage') return 'usage'
  if (tab === 'monetization') return 'monetization'
  return tab
}

const PERCENT_METRICS = new Set([
  'retention_rate',
  'unique_users_percent',
  'unique_users_cumulative_percent',
])

/**
 * Format a metric value for display.
 * - Percentage metrics → e.g. "42.35%"
 * - Revenue metrics    → e.g. "$0.00001234" (dynamic decimals)
 * - All others         → dynamic decimal (4–8 places)
 */
function formatValue(value, metric) {
  if (value === null || value === undefined) return '—'
  if (PERCENT_METRICS.has(metric)) {
    return `${(Number(value) * 100).toFixed(2)}%`
  }
  if (metric.includes('revenue')) {
    return `$${formatDynamic(value)}`
  }
  return formatDynamic(value)
}

/**
 * Given the result object, find which test produced the selected p_value
 * and return a human-readable label, e.g. "p-value (Mann-Whitney U)".
 */
function getPValueLabel(result) {
  if (!result || result.p_value === null || result.p_value === undefined) {
    return 'p-value'
  }
  if (!result.tests || result.tests.length === 0) {
    return 'p-value'
  }

  // First try exact match, then tolerance match (floating-point rounding)
  const match =
    result.tests.find(t => t.p_value === result.p_value) ||
    result.tests.find(t => Math.abs(t.p_value - result.p_value) < 1e-6)

  if (!match) return 'p-value'

  // Convert snake_case name to Title Case
  const readable = match.name
    .split('_')
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ')

  return `p-value (${readable})`
}

export default function ComparePane({
  isOpen,
  onClose,
  tab,              // 'retention' | 'usage' | 'monetization'
  maxDay,
  defaultMetric,    // optional initial metric value
  currentEvent,     // for usage tab: the event that's currently selected in UsageTable
}) {
  const paneRef = useRef(null)
  const [cohorts, setCohorts] = useState([])
  // cohortA = Variant, cohortB = Baseline
  const [cohortA, setCohortA] = useState('')
  const [cohortB, setCohortB] = useState('')
  const [selectedMetric, setSelectedMetric] = useState('')
  const [selectedDay, setSelectedDay] = useState(maxDay)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [result, setResult] = useState(null)
  const [showTestDetails, setShowTestDetails] = useState(false)

  const metrics = getMetricsForTab(tab)

  // Load cohorts on mount / open
  useEffect(() => {
    if (!isOpen) return
    listCohorts()
      .then(data => setCohorts(data.cohorts || []))
      .catch(() => setCohorts([]))
  }, [isOpen])

  // Reset state when the pane opens or tab changes
  useEffect(() => {
    if (!isOpen) return
    setCohortA('')
    setCohortB('')
    setResult(null)
    setError('')
    setShowTestDetails(false)
    setSelectedDay(maxDay)
    // Default metric: use the provided one or the first available
    const availableMetrics = getMetricsForTab(tab)
    if (defaultMetric && availableMetrics.some(m => m.value === defaultMetric)) {
      setSelectedMetric(defaultMetric)
    } else {
      setSelectedMetric(availableMetrics[0]?.value || '')
    }
  }, [isOpen, tab, defaultMetric, maxDay])

  // Keep day capped within 0..maxDay
  useEffect(() => {
    setSelectedDay(prev => Math.min(prev, maxDay))
  }, [maxDay])

  // ESC key handler
  useEffect(() => {
    if (!isOpen) return
    const handler = (e) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [isOpen, onClose])

  // Click-outside handler
  const handleOverlayClick = useCallback((e) => {
    if (paneRef.current && !paneRef.current.contains(e.target)) {
      onClose()
    }
  }, [onClose])

  const metricDef = metrics.find(m => m.value === selectedMetric)
  const needsEvent = metricDef?.requiresEvent ?? false

  const canRun = !!(
    cohortA &&
    cohortB &&
    selectedMetric &&
    selectedDay >= 0 &&
    (!needsEvent || currentEvent)
  )

  const handleRun = async () => {
    setLoading(true)
    setError('')
    setResult(null)
    try {
      const res = await compareCohorts({
        cohort_a: Number(cohortA),
        cohort_b: Number(cohortB),
        tab: getBackendTab(tab),
        metric: selectedMetric,
        day: Number(selectedDay),
        event: needsEvent ? currentEvent : null,
      })
      setResult(res)
    } catch (err) {
      setError(err.message || 'Comparison failed')
    } finally {
      setLoading(false)
    }
  }

  /**
   * Swap Variant ↔ Baseline.
   * Captures both values in local consts before any setter fires so
   * neither setter reads stale/partially-updated state.
   * Does NOT auto-run — user must click "Run Comparison".
   */
  const handleSwap = () => {
    const a = cohortA
    const b = cohortB
    setCohortA(b)
    setCohortB(a)
    setResult(null)
  }

  // Visible cohorts: active and not marked as hidden in the cohort data
  const visibleCohorts = cohorts.filter(
    c => c.is_active && !c.hidden,
  )

  // Each dropdown excludes the other's selection
  const cohortsForVariant  = visibleCohorts.filter(c => String(c.cohort_id) !== String(cohortB))
  const cohortsForBaseline = visibleCohorts.filter(c => String(c.cohort_id) !== String(cohortA))

  // Auto-select default cohorts when the pane opens and when visible cohort count changes.
  useEffect(() => {
    if (!isOpen) return

    if (visibleCohorts.length >= 2) {
      setCohortA(String(visibleCohorts[0].cohort_id))
      setCohortB(String(visibleCohorts[1].cohort_id))
    } else if (visibleCohorts.length === 1) {
      setCohortA(String(visibleCohorts[0].cohort_id))
      setCohortB('')
    }
  }, [isOpen, visibleCohorts.length])

  const dayOptions = Array.from({ length: maxDay + 1 }, (_, i) => i)

  const pValueLabel = getPValueLabel(result)

  return (
    <>
      {/* Overlay */}
      <div
        className={`compare-pane-overlay ${isOpen ? 'open' : ''}`}
        onClick={handleOverlayClick}
        aria-hidden="true"
      />

      {/* Slide-in pane */}
      <aside
        ref={paneRef}
        className={`compare-pane ${isOpen ? 'open' : ''}`}
        role="complementary"
        aria-label="Compare Cohorts"
        data-testid="compare-pane"
      >
        {/* Header */}
        <div className="compare-pane-header">
          <h3>Compare Cohorts</h3>
          <button
            type="button"
            className="compare-pane-close"
            onClick={onClose}
            aria-label="Close compare pane"
            data-testid="compare-pane-close"
          >
            ✕
          </button>
        </div>

        {/* ── Row 1: Baseline ── */}
        <div className="compare-cohort-row">
          <label className="compare-cohort-label" htmlFor="compare-cohort-b">Baseline</label>
          <select
            id="compare-cohort-b"
            className="compare-select"
            value={cohortB}
            onChange={e => { setCohortB(e.target.value); setResult(null) }}
            data-testid="compare-cohort-b"
          >
            <option value="">Select cohort…</option>
            {cohortsForBaseline.map(c => (
              <option key={c.cohort_id} value={c.cohort_id}>{c.cohort_name}</option>
            ))}
          </select>
        </div>

        {/* ── Row 2: Variant + Swap ── */}
        <div className="compare-cohort-row">
          <label className="compare-cohort-label" htmlFor="compare-cohort-a">Variant</label>
          <select
            id="compare-cohort-a"
            className="compare-select"
            value={cohortA}
            onChange={e => { setCohortA(e.target.value); setResult(null) }}
            data-testid="compare-cohort-a"
          >
            <option value="">Select cohort…</option>
            {cohortsForVariant.map(c => (
              <option key={c.cohort_id} value={c.cohort_id}>{c.cohort_name}</option>
            ))}
          </select>
          <button
            type="button"
            className="compare-swap-button"
            onClick={handleSwap}
            aria-label="Swap Variant and Baseline"
            title="Swap Variant and Baseline"
            data-testid="compare-swap-button"
          >
            ⇄
          </button>
        </div>

        {/* Metric + Day selectors */}
        <div className="compare-controls-row">
          {tab !== 'retention' && (
            <label className="compare-control-label">
              Metric
              <select
                className="compare-select"
                value={selectedMetric}
                onChange={e => { setSelectedMetric(e.target.value); setResult(null) }}
                data-testid="compare-metric-select"
              >
                {metrics.map(m => (
                  <option key={m.value} value={m.value}>{m.label}</option>
                ))}
              </select>
            </label>
          )}

          <label className="compare-control-label">
            Day
            <select
              className="compare-select"
              value={selectedDay}
              onChange={e => { setSelectedDay(Number(e.target.value)); setResult(null) }}
              data-testid="compare-day-select"
            >
              {dayOptions.map(d => (
                <option key={d} value={d}>Day {d}</option>
              ))}
            </select>
          </label>
        </div>

        {needsEvent && !currentEvent && (
          <p className="compare-hint">⚠ Select an event in the Usage table to enable comparison.</p>
        )}

        <button
          type="button"
          className="button button-primary compare-run-button"
          onClick={handleRun}
          disabled={!canRun || loading}
          data-testid="compare-run-button"
        >
          {loading ? 'Running…' : 'Run Comparison'}
        </button>

        {/* Error */}
        {error && <p className="error compare-error">{error}</p>}

        {/* Results */}
        {result && (
          <div className="compare-results" data-testid="compare-results">
            <div className="compare-results-header">
              <span className="compare-results-title">Statistical Comparison</span>
            </div>

            <p className="compare-metric-label">{result.metric_label}</p>

            <div className="compare-values-grid">
              {/* Variant (cohortA) */}
              <div className="compare-value-row">
                <span className="compare-value-cohort">
                  Variant
                  {(() => {
                    const c = cohorts.find(c => String(c.cohort_id) === String(cohortA))
                    return c ? ` (${c.cohort_name})` : ''
                  })()}
                </span>
                <span className="compare-value-number" data-testid="compare-value-a">
                  {formatValue(result.cohort_a_value, selectedMetric)}
                </span>
              </div>
              {/* Baseline (cohortB) */}
              <div className="compare-value-row">
                <span className="compare-value-cohort">
                  Baseline
                  {(() => {
                    const c = cohorts.find(c => String(c.cohort_id) === String(cohortB))
                    return c ? ` (${c.cohort_name})` : ''
                  })()}
                </span>
                <span className="compare-value-number" data-testid="compare-value-b">
                  {formatValue(result.cohort_b_value, selectedMetric)}
                </span>
              </div>
            </div>

            <div className="compare-stats-list">
              <div className="compare-stat-row">
                <span>Difference</span>
                <span className={result.difference > 0 ? 'compare-positive' : result.difference < 0 ? 'compare-negative' : ''}>
                  {result.difference >= 0 ? '+' : ''}{formatValue(result.difference, selectedMetric)}
                </span>
              </div>
              {result.relative_lift !== null && result.relative_lift !== undefined && (
                <div className="compare-stat-row">
                  <span>Relative Lift</span>
                  <span className={result.relative_lift > 0 ? 'compare-positive' : result.relative_lift < 0 ? 'compare-negative' : ''}>
                    {result.relative_lift >= 0 ? '+' : ''}{(result.relative_lift * 100).toFixed(2)}%
                  </span>
                </div>
              )}
              <div className="compare-stat-row">
                <span>{pValueLabel}</span>
                <span data-testid="compare-p-value">
                  {result.p_value === null || result.p_value === undefined
                    ? '—'
                    : formatDynamic(result.p_value)}
                </span>
              </div>
              <div className="compare-significance-badge">
                {result.significant
                  ? <span className="compare-sig compare-sig-yes" data-testid="compare-significant">✓ Statistically significant <small>(p &lt; 0.05)</small></span>
                  : <span className="compare-sig compare-sig-no" data-testid="compare-not-significant">✗ Not significant <small>(p ≥ 0.05)</small></span>
                }
              </div>
            </div>

            {/* Expandable test details */}
            <button
              type="button"
              className="compare-details-toggle"
              onClick={() => setShowTestDetails(prev => !prev)}
              aria-expanded={showTestDetails}
              data-testid="compare-test-details-toggle"
            >
              Test Details {showTestDetails ? '▾' : '▸'}
            </button>

            {showTestDetails && (
              <div className="compare-test-details" data-testid="compare-test-details">
                {result.tests.map(t => (
                  <div key={t.name} className="compare-test-row">
                    <span className="compare-test-name">{t.name.replace(/_/g, ' ')}</span>
                    <span className="compare-test-pvalue">
                      p = {t.p_value === null || t.p_value === undefined
                        ? '—'
                        : formatDynamic(t.p_value)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </aside>
    </>
  )
}
