const BASE_URL =
  import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000'

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

export async function listCohorts() {
  return request('/cohorts', { method: 'GET' })
}

export async function updateCohort(cohortId, payload) {
  return request(`/cohorts/${cohortId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function applyFilters(payload) {
  return request('/apply-filters', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function getScope() {
  return request('/scope', { method: 'GET' })
}

export async function getColumns() {
  return request('/columns', { method: 'GET' })
}

export async function getColumnValues(column, eventName) {
  let path = `/column-values?column=${encodeURIComponent(column)}`
  if (eventName) {
    path += `&event_name=${encodeURIComponent(eventName)}`
  }

  return request(path, { method: 'GET' })
}

export async function getDateRange() {
  return request('/date-range', { method: 'GET' })
}

export async function deleteCohort(cohortId) {
  return request(`/cohorts/${cohortId}`, {
    method: 'DELETE',
  })
}

export async function toggleCohortHide(cohortId) {
  return request(`/cohorts/${cohortId}/hide`, {
    method: 'PATCH',
  })
}

export async function getRetention(maxDay, retentionEvent = 'any', includeCI = false, confidence = 0.95) {
  const query = new URLSearchParams({
    max_day: String(maxDay),
    include_ci: String(includeCI),
    confidence: String(confidence),
  })

  let path = `/retention?${query.toString()}`

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

export async function getUsage(event, maxDay, retentionEvent) {
  if (retentionEvent === undefined || retentionEvent === null || retentionEvent === '') {
    throw new Error('Retention event must be selected before loading usage metrics')
  }

  let path = `/usage?event=${encodeURIComponent(event)}&max_day=${maxDay}`
  if (retentionEvent !== 'any') {
    path += `&retention_event=${encodeURIComponent(retentionEvent)}`
  }

  return request(path, { method: 'GET' })
}


export async function getRevenueEvents() {
  return request('/revenue-events', { method: 'GET' })
}

export async function updateRevenueEvents(events) {
  return request('/revenue-events', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ events }),
  })
}

export async function getMonetization(maxDay) {
  return request(`/monetization?max_day=${maxDay}`, { method: 'GET' })
}
