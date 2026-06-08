import { describe, it, expect } from 'vitest'
import { formatCohortLogic } from './cohortUtils'

describe('formatCohortLogic timestamp filters', () => {
  it('renders ON payload without object stringification', () => {
    const text = formatCohortLogic({
      logic_operator: 'AND',
      join_type: 'first_event',
      conditions: [
        {
          event_name: 'InstallSuccess',
          min_event_count: 1,
          property_filter: {
            column: 'event_time',
            operator: 'ON',
            values: { date: '2026-03-15' },
          },
        },
      ],
    })

    expect(text).toContain('event_time ON 2026-03-15')
    expect(text).not.toContain('[object Object]')
  })

  it('renders BETWEEN payload with date-time range', () => {
    const text = formatCohortLogic({
      logic_operator: 'AND',
      join_type: 'condition_met',
      conditions: [
        {
          event_name: 'purchase',
          min_event_count: 1,
          property_filter: {
            column: 'event_time',
            operator: 'BETWEEN',
            values: {
              startDate: '2026-03-01',
              startTime: '00:00:00',
              endDate: '2026-03-31',
              endTime: '23:59:59',
            },
          },
        },
      ],
    })

    expect(text).toContain('2026-03-01 00:00:00 → 2026-03-31 23:59:59')
  })
})
