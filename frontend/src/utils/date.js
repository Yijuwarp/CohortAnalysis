export function formatPrettyDate(dateString) {
  if (!dateString) {
    return ''
  }

  const date = new Date(dateString)
  if (Number.isNaN(date.getTime())) {
    return ''
  }

  const day = date.getDate()
  const suffix =
    day % 10 === 1 && day !== 11
      ? 'st'
      : day % 10 === 2 && day !== 12
        ? 'nd'
        : day % 10 === 3 && day !== 13
          ? 'rd'
          : 'th'

  const formatter = new Intl.DateTimeFormat('en-US', {
    month: 'short',
    year: 'numeric',
  })

  return `${day}${suffix} ${formatter.format(date)}`
}
