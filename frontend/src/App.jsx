import { useState } from 'react'
import Upload from './components/Upload'
import Mapping from './components/Mapping'
import FilterData from './components/FilterData'
import CohortForm from './components/CohortForm'
import RetentionTable from './components/RetentionTable'
import UsageTable from './components/UsageTable'

export default function App() {
  const [columns, setColumns] = useState([])
  const [detectedTypes, setDetectedTypes] = useState({})
  const [retentionRefreshToken, setRetentionRefreshToken] = useState(0)
  const [selectedRetentionEvent, setSelectedRetentionEvent] = useState('any')

  const refreshRetention = () => {
    setRetentionRefreshToken((current) => current + 1)
  }

  return (
    <main className="app-container">
      <h1>Cohort Analysis Dashboard</h1>
      <Upload onUploaded={(newColumns, types) => { setColumns(newColumns); setDetectedTypes(types) }} />
      <Mapping columns={columns} detectedTypes={detectedTypes} onMappingComplete={refreshRetention} />
      <FilterData refreshToken={retentionRefreshToken} onFiltersApplied={refreshRetention} />
      <CohortForm refreshToken={retentionRefreshToken} onCohortsChanged={refreshRetention} />
      <RetentionTable
        refreshToken={retentionRefreshToken}
        retentionEvent={selectedRetentionEvent}
        onRetentionEventChange={setSelectedRetentionEvent}
      />
      <UsageTable refreshToken={retentionRefreshToken} retentionEvent={selectedRetentionEvent} />
    </main>
  )
}
