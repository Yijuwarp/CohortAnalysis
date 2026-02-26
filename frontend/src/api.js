const BASE_URL = 'http://127.0.0.1:8000'

async function request(path, options = {}) {
  const response = await fetch(`${BASE_URL}${path}`, options)
  const data = await response.json()

  if (!response.ok) {
    const message = data?.detail || 'Request failed'
    throw new Error(message)
  }

  return data
}

export async function uploadCSV(file) {
  const formData = new FormData()
  formData.append('file', file)

  return request('/upload', {
    method: 'POST',
    body: formData,
  })
}

export async function mapColumns(payload) {
  return request('/map-columns', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function createCohort(payload) {
  return request('/cohorts', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function deleteCohort(cohortId) {
  return request(`/cohorts/${cohortId}`, {
    method: 'DELETE',
  })
}

export async function getRetention(maxDay, retentionEvent = 'any') {
  let path = `/retention?max_day=${maxDay}`

  if (retentionEvent !== 'any') {
    path += `&retention_event=${encodeURIComponent(retentionEvent)}`
  }

  return request(path, {
    method: 'GET',
  })
}

export async function listEvents() {
  return request('/events', {
    method: 'GET',
  })
}

export async function getUsage(event, maxDay) {
  return request(
    `/usage?event=${encodeURIComponent(event)}&max_day=${maxDay}`,
    { method: 'GET' }
  )
}
