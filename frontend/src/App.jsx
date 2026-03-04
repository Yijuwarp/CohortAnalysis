import { useState } from 'react'
import Upload from './components/Upload'
import Mapping from './components/Mapping'
import FilterData from './components/FilterData'
import CohortForm from './components/CohortForm'
import RetentionTable from './components/RetentionTable'
import UsageTable from './components/UsageTable'
import MonetizationTable from './components/MonetizationTable'

export default function App() {
  const [columns, setColumns] = useState([])
  const [detectedTypes, setDetectedTypes] = useState({})
  const [suggestedMappings, setSuggestedMappings] = useState(null)
  const [retentionRefreshToken, setRetentionRefreshToken] = useState(0)
  const [selectedRetentionEvent, setSelectedRetentionEvent] = useState('any')

  const refreshRetention = () => {
    setRetentionRefreshToken((current) => current + 1)
  }

  return (
    <main className="app-container">
      <h1>Cohort Explorer</h1>
      <Upload onUploaded={(newColumns, types, suggestions) => { setColumns(newColumns); setDetectedTypes(types); setSuggestedMappings(suggestions) }} />
      <Mapping
        columns={columns}
        detectedTypes={detectedTypes}
        suggestedMappings={suggestedMappings}
        onMappingComplete={refreshRetention}
      />
      <FilterData refreshToken={retentionRefreshToken} onFiltersApplied={refreshRetention} />
      <CohortForm refreshToken={retentionRefreshToken} onCohortsChanged={refreshRetention} />
      <RetentionTable
        refreshToken={retentionRefreshToken}
        retentionEvent={selectedRetentionEvent}
        onRetentionEventChange={setSelectedRetentionEvent}
      />
      <UsageTable refreshToken={retentionRefreshToken} retentionEvent={selectedRetentionEvent} />
      <MonetizationTable refreshToken={retentionRefreshToken} />
    </main>
  )
}
