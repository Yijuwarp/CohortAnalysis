import React, { useEffect, useState, useCallback } from 'react'
import { getColumns, getColumnValues, splitCohort, previewSplit } from '../api'

const formatCount = (n) => {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return String(n)
}

export default function CohortSplitModal({ cohort, onClose, onSplitDone }) {
  const [tab, setTab] = useState('random') // 'random' | 'property'

  // Random split state
  const [numGroups, setNumGroups] = useState(4)

  // Property split state
  const [columns, setColumns] = useState([])
  const [selectedColumn, setSelectedColumn] = useState('')
  const [columnValues, setColumnValues] = useState([])
  const [selectedValues, setSelectedValues] = useState([])
  const [loadingValues, setLoadingValues] = useState(false)

  // Preview state
  const [preview, setPreview] = useState(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  const [previewError, setPreviewError] = useState('')

  // Split state
  const [splitting, setSplitting] = useState(false)
  const [splitError, setSplitError] = useState('')

  // Load property columns on mount
  useEffect(() => {
    getColumns()
      .then((data) => {
        const cols = data?.columns || []
        setColumns(cols)
        if (cols.length > 0) setSelectedColumn(cols[0].name || '')
      })
      .catch(() => { /* ignore */ })
  }, [])

  // Load column values when column changes
  useEffect(() => {
    if (!selectedColumn || tab !== 'property') return
    setLoadingValues(true)
    setColumnValues([])
    setSelectedValues([])
    setPreview(null)
    getColumnValues(selectedColumn)
      .then((data) => {
        const vals = data?.values?.map(v => String(v)) || []
        setColumnValues(vals)
      })
      .catch(() => setColumnValues([]))
      .finally(() => setLoadingValues(false))
  }, [selectedColumn, tab])

  // Clear preview when inputs change
  useEffect(() => { setPreview(null) }, [tab, numGroups, selectedColumn, selectedValues])

  const buildPayload = useCallback(() => {
    if (tab === 'random') {
      return { type: 'random', random: { num_groups: numGroups } }
    }
    return {
      type: 'property',
      property: { column: selectedColumn, values: selectedValues },
    }
  }, [tab, numGroups, selectedColumn, selectedValues])

  const handlePreview = async () => {
    if (tab === 'property' && selectedValues.length === 0) {
      setPreviewError('Select at least one value')
      return
    }
    setPreviewLoading(true)
    setPreviewError('')
    setPreview(null)
    try {
      const res = await previewSplit(cohort.cohort_id, buildPayload())
      setPreview(res.preview || [])
    } catch (err) {
      setPreviewError(err.message)
    } finally {
      setPreviewLoading(false)
    }
  }

  const handleConfirm = async () => {
    if (tab === 'property' && selectedValues.length === 0) {
      setSplitError('Select at least one value')
      return
    }
    setSplitting(true)
    setSplitError('')
    try {
      await splitCohort(cohort.cohort_id, buildPayload())
      onSplitDone()
      onClose()
    } catch (err) {
      setSplitError(err.message)
    } finally {
      setSplitting(false)
    }
  }

  const toggleValue = (val) => {
    setSelectedValues(prev =>
      prev.includes(val) ? prev.filter(v => v !== val) : [...prev, val]
    )
  }

  const toggleAll = () => {
    setSelectedValues(prev =>
      prev.length === columnValues.length ? [] : [...columnValues]
    )
  }

  const allSelected = columnValues.length > 0 && selectedValues.length === columnValues.length
  const confirmDisabled = splitting || (tab === 'property' && selectedValues.length === 0)
  const previewDisabled = previewLoading || (tab === 'property' && selectedValues.length === 0)

  return (
    <div className="modal-overlay" onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div className="modal-content split-modal" role="dialog" aria-modal="true" aria-label="Split cohort">
        <div className="modal-header">
          <h2>Split Cohort</h2>
          <span className="split-modal-parent-label">
            Parent: <strong>{cohort.cohort_name || cohort.name}</strong>
          </span>
          <button className="modal-close-btn" onClick={onClose} aria-label="Close">✕</button>
        </div>

        {/* Tabs */}
        <div className="split-tab-bar">
          <button
            className={`split-tab${tab === 'random' ? ' active' : ''}`}
            onClick={() => { setTab('random'); setPreview(null); setPreviewError(''); setSplitError('') }}
          >
            🎲 Random Split
          </button>
          <button
            className={`split-tab${tab === 'property' ? ' active' : ''}`}
            onClick={() => { setTab('property'); setPreview(null); setPreviewError(''); setSplitError('') }}
          >
            🏷 Property Split
          </button>
        </div>

        <div className="split-modal-body">
          {tab === 'random' && (
            <div className="split-random-config">
              <label className="split-field-label" htmlFor="num-groups-input">Number of groups</label>
              <div className="split-num-groups-row">
                <input
                  id="num-groups-input"
                  type="number"
                  min={1}
                  max={10}
                  value={numGroups}
                  onChange={e => setNumGroups(Math.min(10, Math.max(1, Number(e.target.value))))}
                  className="split-num-input"
                />
                <span className="split-hint">Creates <strong>{numGroups}</strong> cohorts named <em>{cohort.cohort_name || cohort.name}_Random_1</em>, …</span>
              </div>
            </div>
          )}

          {tab === 'property' && (
            <div className="split-property-config">
              <label className="split-field-label" htmlFor="split-column-sel">Property column</label>
              <select
                id="split-column-sel"
                className="split-column-select"
                value={selectedColumn}
                onChange={e => setSelectedColumn(e.target.value)}
              >
                {Array.isArray(columns) && columns.map(col => (
                  <option key={col.name} value={col.name}>{col.name}</option>
                ))}
              </select>

              {loadingValues ? (
                <p className="secondary-text split-loading">Loading values…</p>
              ) : columnValues.length === 0 ? (
                <p className="secondary-text">No values found for this column.</p>
              ) : (
                <div className="split-values-section">
                  <div className="split-values-header">
                    <span className="split-field-label">Values</span>
                    <button className="split-select-all-btn" onClick={toggleAll}>
                      {allSelected ? 'Deselect All' : 'Select All'}
                    </button>
                  </div>
                  <div className="split-values-list">
                    {Array.isArray(columnValues) && columnValues.map(val => (
                      <label key={val} className="split-value-row">
                        <input
                          type="checkbox"
                          checked={selectedValues.includes(val)}
                          onChange={() => toggleValue(val)}
                        />
                        <span className="split-value-label">{val}</span>
                      </label>
                    ))}
                  </div>
                  {selectedValues.length > 0 && !allSelected && (
                    <p className="split-other-note secondary-text">
                      ℹ An <em>_other</em> cohort will be created for remaining values (if any users exist).
                    </p>
                  )}
                </div>
              )}
            </div>
          )}

          {/* Preview section */}
          <div className="split-preview-section">
            <button
              className="button button-secondary split-preview-btn"
              onClick={handlePreview}
              disabled={previewDisabled}
            >
              {previewLoading ? 'Computing…' : '👁 Preview'}
            </button>

            {previewError && <p className="error" style={{ marginTop: '8px' }}>{previewError}</p>}

            {preview && preview.length > 0 && (
              <div className="split-preview-result">
                <p className="split-preview-title">Will create <strong>{preview.length}</strong> cohort{preview.length !== 1 ? 's' : ''}:</p>
                <ul className="split-preview-list">
                  {Array.isArray(preview) && preview.map((item) => (
                    <li key={item.name} className="split-preview-item">
                      <span className="split-preview-name">{item.name}</span>
                      <span className="split-preview-count">{formatCount(item.count)} users</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {preview && preview.length === 0 && (
              <p className="secondary-text" style={{ marginTop: '8px' }}>No cohorts would be created (no matching users).</p>
            )}
          </div>

          {splitError && <p className="error" style={{ marginTop: '8px' }}>{splitError}</p>}
        </div>

        <div className="modal-footer split-modal-footer">
          <button className="button button-secondary" onClick={onClose} disabled={splitting}>
            Cancel
          </button>
          <button
            className="button button-primary"
            onClick={handleConfirm}
            disabled={confirmDisabled}
          >
            {splitting ? 'Creating…' : 'Create Split'}
          </button>
        </div>
      </div>
    </div>
  )
}
