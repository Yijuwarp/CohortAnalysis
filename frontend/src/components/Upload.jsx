import { useState } from 'react'
import { uploadCSV } from '../api'

export default function Upload({ onUploaded }) {
  const [file, setFile] = useState(null)
  const [result, setResult] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  const handleUpload = async () => {
    if (!file) {
      setError('Please select a CSV file first.')
      return
    }

    setLoading(true)
    setError('')

    try {
      const data = await uploadCSV(file)
      setResult(data)
      onUploaded(data.columns, data.detected_types || {})
    } catch (err) {
      setError(err.message)
      setResult(null)
    } finally {
      setLoading(false)
    }
  }

  return (
    <section className="card">
      <h2>1. Upload CSV</h2>
      <div className="inline-controls">
        <input type="file" accept=".csv" onChange={(e) => setFile(e.target.files?.[0] || null)} />
        <button className="button button-primary" onClick={handleUpload} disabled={loading}>{loading ? 'Uploading...' : 'Upload'}</button>
      </div>
      {error && <p className="error">{error}</p>}
      {result && (
        <div>
          <p className="secondary-text">Rows imported: {result.rows_imported}</p>
          <p className="secondary-text">Columns: {result.columns.join(', ')}</p>
        </div>
      )}
    </section>
  )
}
