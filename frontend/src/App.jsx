import { useState } from 'react'
import Upload from './components/Upload'
import Mapping from './components/Mapping'
import CohortForm from './components/CohortForm'
import RetentionTable from './components/RetentionTable'

export default function App() {
  const [columns, setColumns] = useState([])
  const [retentionRefreshToken, setRetentionRefreshToken] = useState(0)

  const refreshRetention = () => {
    setRetentionRefreshToken((current) => current + 1)
  }

  return (
    <main className="container">
      <h1>Cohort Analysis Dashboard</h1>
      <Upload onUploaded={setColumns} />
      <Mapping columns={columns} />
      <CohortForm onCohortsChanged={refreshRetention} />
      <RetentionTable refreshToken={retentionRefreshToken} />
    </main>
  )
}
