import { useState } from 'react'
import Upload from './components/Upload'
import Mapping from './components/Mapping'
import FilterData from './components/FilterData'
import CohortForm from './components/CohortForm'
import RetentionTable from './components/RetentionTable'
import UsageTable from './components/UsageTable'

export default function App() {
  const [columns, setColumns] = useState([])
  const [retentionRefreshToken, setRetentionRefreshToken] = useState(0)

  const refreshRetention = () => {
    setRetentionRefreshToken((current) => current + 1)
  }

  return (
    <main className="app-container">
      <h1>Cohort Analysis Dashboard</h1>
      <Upload onUploaded={setColumns} />
      <Mapping columns={columns} onMappingComplete={refreshRetention} />
      <FilterData refreshToken={retentionRefreshToken} onFiltersApplied={refreshRetention} />
      <CohortForm refreshToken={retentionRefreshToken} onCohortsChanged={refreshRetention} />
      <RetentionTable refreshToken={retentionRefreshToken} />
      <UsageTable refreshToken={retentionRefreshToken} />
    </main>
  )
}
