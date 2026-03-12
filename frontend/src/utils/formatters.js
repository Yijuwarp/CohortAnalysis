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
