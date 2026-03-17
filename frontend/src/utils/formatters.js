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
