import React, { useState } from 'react'
import { exportToExcel, exportToZip, triggerDownload } from '../utils/exportUtils'

export default function ExportModal({ exportBuffer, onClose, onClearAll }) {
  const [format, setFormat] = useState('xlsx')
  const [exporting, setExporting] = useState(false)
  const [error, setError] = useState(null)

  const handleDownload = async () => {
    setExporting(true)
    setError(null)

    try {
      let result
      if (format === 'xlsx') {
        result = await exportToExcel(exportBuffer)
      } else {
        result = await exportToZip(exportBuffer, format) // format: csv or json
      }
      triggerDownload(result.blob, result.filename)
      onClearAll()
      onClose()
    } catch (err) {
      setError(err.message)
    } finally {
      setExporting(false)
    }
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content export-modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Export Analysis ({exportBuffer.length} items)</h3>
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        <div className="modal-body">
          <div className="export-format-selector">
            <p>Select Export Format:</p>
            <label className="radio-option">
              <input 
                type="radio" 
                name="format" 
                value="xlsx" 
                checked={format === 'xlsx'} 
                onChange={(e) => setFormat(e.target.value)} 
              />
              <span>Excel (.xlsx)</span>
            </label>
            <label className="radio-option">
              <input 
                type="radio" 
                name="format" 
                value="csv" 
                checked={format === 'csv'} 
                onChange={(e) => setFormat(e.target.value)} 
              />
              <span>CSV (.zip)</span>
            </label>
            <label className="radio-option">
              <input 
                type="radio" 
                name="format" 
                value="json" 
                checked={format === 'json'} 
                onChange={(e) => setFormat(e.target.value)} 
              />
              <span>JSON (.zip)</span>
            </label>
          </div>

          {error && <p className="error">{error}</p>}
        </div>

        <div className="modal-footer">
          <button 
            className="button button-secondary" 
            onClick={onClearAll}
            disabled={exporting}
          >
            Clear All
          </button>
          <div className="footer-right">
            <button 
              className="button button-ghost" 
              onClick={onClose}
              disabled={exporting}
            >
              Cancel
            </button>
            <button 
              className="button button-primary" 
              onClick={handleDownload}
              disabled={exporting || exportBuffer.length === 0}
            >
              {exporting ? 'Generating...' : 'Download'}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
