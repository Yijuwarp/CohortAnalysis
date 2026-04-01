export async function exportToExcel(exportBuffer) {
  return generateExportBlob(exportBuffer, 'excel')
}

export async function exportToZip(exportBuffer, format) {
  return generateExportBlob(exportBuffer, `zip-${format}`) // format: csv or json
}

function generateExportBlob(exportBuffer, format) {
  return new Promise((resolve, reject) => {
    // Vite Web Worker specific setup
    const worker = new Worker(new URL('../workers/exportWorker.js', import.meta.url), { type: 'module' })
    
    worker.onmessage = (e) => {
      const { success, blob, error, filename } = e.data
      if (success) {
        resolve({ blob, filename })
      } else {
        reject(new Error(error || 'Export worker failed'))
      }
      worker.terminate()
    }

    worker.onerror = (err) => {
      reject(new Error(`Worker error: ${err.message}`))
      worker.terminate()
    }

    worker.postMessage({ exportBuffer, format })
  })
}

export function triggerDownload(blob, filename) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}
