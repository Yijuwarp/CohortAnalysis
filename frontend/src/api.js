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

function normalizeMaxDay(value) {
  const n = Number(value)
  if (!Number.isFinite(n) || n <= 0) {
    return 7
  }

  return Math.floor(n)
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

export async function randomSplitCohort(cohortId) {
  return request(`/cohorts/${cohortId}/random_split`, {
    method: 'POST',
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

export async function getUsage(event, maxDay, retentionEvent, propertyFilter = null) {
  if (retentionEvent === undefined || retentionEvent === null || retentionEvent === '') {
    throw new Error('Retention event must be selected before loading usage metrics')
  }

  let path = `/usage?event=${encodeURIComponent(event)}&max_day=${maxDay}`
  if (retentionEvent !== 'any') {
    path += `&retention_event=${encodeURIComponent(retentionEvent)}`
  }
  if (propertyFilter?.property) {
    path += `&property=${encodeURIComponent(propertyFilter.property)}`
    path += `&operator=${encodeURIComponent(propertyFilter.operator || '=')}`
    if (propertyFilter.value !== undefined && propertyFilter.value !== null && propertyFilter.value !== '') {
      path += `&value=${encodeURIComponent(propertyFilter.value)}`
    }
  }

  return request(path, { method: 'GET' })
}

export async function getEventProperties(event) {
  return request(`/events/${encodeURIComponent(event)}/properties`, { method: 'GET' })
}

export async function getEventPropertyValues(event, property, limit = 25) {
  return request(
    `/events/${encodeURIComponent(event)}/properties/${encodeURIComponent(property)}/values?limit=${limit}`,
    { method: 'GET' }
  )
}


export async function getRevenueEvents() {
  return request('/revenue-events', { method: 'GET' })
}

export async function getRevenueConfigEvents() {
  return request('/revenue-config-events', { method: 'GET' })
}

export async function updateRevenueConfig(revenueConfig) {
  return request('/update-revenue-config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ revenue_config: revenueConfig }),
  })
}

export async function getMonetization(maxDay) {
  const safeMaxDay = normalizeMaxDay(maxDay)
  return request(`/monetization?max_day=${safeMaxDay}`, { method: 'GET' })
}

export async function getUsageFrequency(event, propertyFilter = null) {
  let path = `/usage-frequency?event=${encodeURIComponent(event)}`
  if (propertyFilter?.property) {
    path += `&property=${encodeURIComponent(propertyFilter.property)}`
    path += `&operator=${encodeURIComponent(propertyFilter.operator || '=')}`
    if (propertyFilter.value !== undefined && propertyFilter.value !== null && propertyFilter.value !== '') {
      path += `&value=${encodeURIComponent(propertyFilter.value)}`
    }
  }

  return request(path, { method: 'GET' })
}

export async function compareCohorts(payload) {
  return request('/compare-cohorts', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

// ---------------------------------------------------------------------------
// Funnels
// ---------------------------------------------------------------------------

export async function createFunnel(payload) {
  return request('/funnels', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function updateFunnel(funnelId, payload) {
  return request(`/funnels/${funnelId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

export async function listFunnels() {
  return request('/funnels', { method: 'GET' })
}

export async function deleteFunnel(funnelId) {
  return request(`/funnels/${funnelId}`, { method: 'DELETE' })
}

export async function runFunnel(funnelId) {
  return request('/funnels/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ funnel_id: funnelId }),
  })
}
