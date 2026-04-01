import { useState } from 'react'
import { formatInteger, formatShortNumber } from '../utils/formatters'
import ExportHover from './ExportHover'

const MAX_DAY_OPTIONS = [7, 14, 30, 60, 90, 180, 365]

function formatDatasetFilename(filename = '') {
  const safeName = String(filename || 'Unknown')

  if (safeName.length <= 30) return safeName

  const extIndex = safeName.lastIndexOf('.')

  if (extIndex === -1) {
    return `${safeName.slice(0, 29)}…`
  }

  const name = safeName.slice(0, extIndex)
  const ext = safeName.slice(extIndex)

  const truncatedName = name.slice(0, Math.max(10, 29 - ext.length))

  return `${truncatedName}…${ext}`
}

export default function TopToolbar({
  hasDataset,
  datasetMeta,
  uploading,
  onUploadClick,
  onRemapColumns,
  onOpenRevenueConfig,
  onOpenCreateCohort,
  onToggleFilters,
  activeFilterCount,
  globalMaxDay,
  setGlobalMaxDay,
  exportCount = 0,
  exportBuffer = [],
  onRemoveExportItem,
  onOpenExportModal,
}) {
  const [isHoveringExport, setIsHoveringExport] = useState(false)
  const filename = datasetMeta?.filename || 'Unknown'
  const rows = Number(datasetMeta?.rows || 0)
  const events = Number(datasetMeta?.events || 0)
  const users = Number(datasetMeta?.users || 0)

  const filtersAreActive = activeFilterCount > 0
  const filtersLabel = filtersAreActive ? `Filters (${activeFilterCount})` : 'Filters'

  return (
    <div className="top-toolbar">
      <div className="toolbar-left">
        <button className="button button-primary" onClick={onUploadClick} disabled={uploading}>
          {uploading ? 'Uploading...' : 'Upload CSV'}
        </button>
        <button
          className="button button-secondary"
          onClick={onRemapColumns}
          disabled={!hasDataset}
        >
          Remap Columns
        </button>
        <button
          className="button button-secondary"
          onClick={onOpenRevenueConfig}
          disabled={!hasDataset}
        >
          Revenue Config
        </button>
        <button
          className="button button-primary"
          onClick={onOpenCreateCohort}
          disabled={!hasDataset}
        >
          Create Cohort
        </button>
      </div>

      <div className="toolbar-dataset">
        <span className="toolbar-dataset-filename" title={filename}>{formatDatasetFilename(filename)}</span>
        <span className="toolbar-dot">•</span>
        <span title={`${formatInteger(rows)} rows`}>{formatShortNumber(rows)} rows</span>
        <span className="toolbar-dot">•</span>
        <span title={`${formatInteger(events)} events`}>{formatShortNumber(events)} events</span>
        <span className="toolbar-dot">•</span>
        <span title={`${formatInteger(users)} users`}>{formatShortNumber(users)} users</span>
      </div>

      <div className="toolbar-right">
        <button
          type="button"
          className={`toolbar-filters-button ${filtersAreActive ? 'active' : ''}`}
          disabled={!hasDataset}
          title={filtersAreActive
            ? `Filters active: ${activeFilterCount} — Click to edit filters`
            : 'Click to edit filters'}
          onClick={onToggleFilters}
        >
          {filtersLabel}
        </button>

        <label className="toolbar-max-day">
          <span>Max Day:</span>
          <select
            value={globalMaxDay}
            onChange={(e) => setGlobalMaxDay(Number(e.target.value))}
            disabled={!hasDataset}
          >
            {(MAX_DAY_OPTIONS.includes(globalMaxDay) ? MAX_DAY_OPTIONS : [...MAX_DAY_OPTIONS, globalMaxDay].sort((a,b) => a-b)).map((day) => (
              <option key={day} value={day}>{day}</option>
            ))}
          </select>
        </label>

        <div 
          className="toolbar-export-container"
          onMouseEnter={() => setIsHoveringExport(true)}
          onMouseLeave={() => setIsHoveringExport(false)}
        >
          <button
            type="button"
            className={`button ${exportCount > 0 ? 'button-primary' : 'button-secondary'}`}
            disabled={exportCount === 0}
            onClick={onOpenExportModal}
          >
            Export ({exportCount})
          </button>
          
          {isHoveringExport && exportCount > 0 && (
            <ExportHover 
              exportBuffer={exportBuffer} 
              onRemoveItem={onRemoveExportItem}
            />
          )}
        </div>
      </div>
    </div>
  )
}
