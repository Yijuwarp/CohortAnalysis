export const usdFormatter = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
})

export function formatCurrency(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '-'
  }

  return usdFormatter.format(Number(value))
}

const integerFormatter = new Intl.NumberFormat('en-US')

export function formatInteger(value) {
  return integerFormatter.format(Number(value || 0))
}

export function formatShortNumber(value) {
  const numericValue = Number(value || 0)
  if (numericValue >= 1000000) {
    return `${(numericValue / 1000000).toFixed(2).replace(/\.00$/, '').replace(/(\.\d)0$/, '$1')}M`
  }
  if (numericValue >= 1000) {
    const v = numericValue / 1000
    return `${v.toFixed(1).replace(/\.0$/, '')}k`
  }
  return String(numericValue)
}

/**
 * Dynamic decimal formatter.
 * Starts at 4 decimal places and increases up to 8 until the formatted
 * value is non-zero, so that small-but-meaningful differences are always
 * visible instead of showing as "0.0000".
 */
export function formatDynamic(value) {
  if (value === null || value === undefined) return '—'

  let decimals = 4
  while (decimals <= 8) {
    const formatted = Number(value).toFixed(decimals)
    if (Number(formatted) !== 0 || decimals === 8) {
      return formatted
    }
    decimals++
  }
}
export function formatSplitValue(value) {
  if (typeof value !== "string") return value

  // Detect timestamp ending in midnight
  if (value.match(/\d{4}-\d{2}-\d{2} 00:00:00$/)) {
    return value.replace(" 00:00:00", "")
  }

  return value
}

export function formatDuration(seconds) {
  if (seconds === null || seconds === undefined) return '—'
  if (seconds < 1) return '<1s'

  const s = Math.floor(seconds)
  const m = Math.floor(s / 60)
  const h = Math.floor(s / 3600)
  const d = Math.floor(s / 86400)

  if (s < 60) {
    return `${s}s`
  }
  if (s < 3600) {
    const min = m
    const sec = s % 60
    return sec > 0 ? `${min}m ${sec}s` : `${min}m`
  }
  if (s < 86400) {
    const hour = h
    const min = m % 60
    return min > 0 ? `${hour}h ${min}m` : `${hour}h`
  }
  
  const day = d
  const hour = h % 24
  return hour > 0 ? `${day}d ${hour}h` : `${day}d`
}
