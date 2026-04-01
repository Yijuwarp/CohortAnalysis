import * as XLSX from 'xlsx';
import JSZip from 'jszip';

self.onmessage = async (e) => {
  const { exportBuffer, format } = e.data;

  try {
    let blob;
    let filename;

    if (format === 'excel') {
      blob = await generateExcel(exportBuffer);
      filename = `cohort_analysis_export_${Date.now()}.xlsx`;
    } else if (format === 'zip-csv') {
      blob = await generateZip(exportBuffer, 'csv');
      filename = `cohort_analysis_export_${Date.now()}_csv.zip`;
    } else if (format === 'zip-json') {
      blob = await generateZip(exportBuffer, 'json');
      filename = `cohort_analysis_export_${Date.now()}_json.zip`;
    }

    self.postMessage({ success: true, blob, filename });
  } catch (err) {
    self.postMessage({ success: false, error: err.message });
  }
};

async function generateExcel(exportBuffer) {
  const wb = XLSX.utils.book_new();

  for (let i = 0; i < exportBuffer.length; i++) {
    const item = exportBuffer[i];
    // Sanitize sheet name: max 31 chars, remove forbidden chars
    let sheetName = (item.title || item.type || 'Sheet')
      .replace(/[\[\]\*\?\/\\]/g, '')
      .substring(0, 31);
    
    // Add (1), (2)... if duplicate
    let finalSheetName = sheetName;
    let counter = 1;
    while (wb.SheetNames.includes(finalSheetName)) {
      finalSheetName = `${sheetName.substring(0, 27)}(${counter++})`;
    }

    const rows = [];
    
    // Header Section
    rows.push([item.title]);
    rows.push([item.summary]);
    rows.push([]); // Padding

    // Settings
    if (Object.keys(item.meta.settings || {}).length > 0) {
      rows.push(['Settings:']);
      Object.entries(item.meta.settings).forEach(([k, v]) => {
        rows.push([k, v]);
      });
      rows.push([]);
    }

    // Cohorts
    if (item.meta.cohorts && item.meta.cohorts.length > 0) {
      rows.push(['Cohorts:']);
      item.meta.cohorts.forEach(c => {
        rows.push([c.cohort_name || c.name || 'Unknown Cohort']);
      });
      rows.push([]);
    }

    // Filters
    if (item.meta.filters && item.meta.filters.length > 0) {
      rows.push(['Applied Filters:']);
      item.meta.filters.forEach(f => {
        const val = Array.isArray(f.value) ? f.value.join(', ') : f.value;
        rows.push([`${f.column} ${f.operator} ${val}`]);
      });
      rows.push([]);
    }

    // Tables
    const ws = XLSX.utils.aoa_to_sheet(rows);
    
    let currentRowOffset = rows.length;

    for (const table of item.tables) {
      if (table.title) {
        XLSX.utils.sheet_add_aoa(ws, [[table.title]], { origin: currentRowOffset++ });
      }

      // Add Header Row
      const headers = table.columns.map(c => c.label);
      XLSX.utils.sheet_add_aoa(ws, [headers], { origin: currentRowOffset });

      // Add Data Rows
      const dataRows = table.data.map(rowData => {
        return table.columns.map(col => {
          const val = rowData[col.key];
          return val === undefined ? null : val;
        });
      });

      XLSX.utils.sheet_add_aoa(ws, dataRows, { origin: currentRowOffset + 1 });

      // Apply Cell Formats
      table.data.forEach((rowData, rowIndex) => {
        table.columns.forEach((col, colIndex) => {
          const val = rowData[col.key];
          if (typeof val === 'number') {
            const cellRef = XLSX.utils.encode_cell({ r: currentRowOffset + 1 + rowIndex, c: colIndex });
            if (!ws[cellRef]) return;

            if (col.type === 'percentage') {
              ws[cellRef].z = '0.00%';
            } else if (col.type === 'currency') {
              ws[cellRef].z = '$#,##0.00';
            } else if (col.type === 'number') {
               ws[cellRef].z = '#,##0';
            }
          }
        });
      });

      currentRowOffset += 1 + dataRows.length + 1; // header + data + spacer
    }

    XLSX.utils.book_append_sheet(wb, ws, finalSheetName);
  }

  const out = XLSX.write(wb, { type: 'array', bookType: 'xlsx' });
  return new Blob([out], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
}

async function generateZip(exportBuffer, format) {
  const zip = new JSZip();
  const manifest = [];

  for (let i = 0; i < exportBuffer.length; i++) {
    const item = exportBuffer[i];
    let baseName = (item.title || item.type || 'export').replace(/[^a-z0-9]/gi, '_').toLowerCase();
    
    // Ensure unique filename
    let finalFileName = baseName;
    let counter = 1;
    while (zip.file(`${finalFileName}.${format}`)) {
      finalFileName = `${baseName}_${counter++}`;
    }

    let content;
    if (format === 'json') {
      content = JSON.stringify(item, null, 2);
    } else {
      content = convertToCSV(item);
    }

    zip.file(`${finalFileName}.${format}`, content);
    manifest.push({
      item_id: item.id,
      file: `${finalFileName}.${format}`,
      title: item.title,
      type: item.type,
      meta: item.meta
    });
  }

  zip.file('manifest.json', JSON.stringify(manifest, null, 2));
  return await zip.generateAsync({ type: 'blob' });
}

function convertToCSV(item) {
  let csv = `"${item.title.replace(/"/g, '""')}"\n`;
  csv += `"${item.summary.replace(/"/g, '""')}"\n\n`;

  // Settings
  if (item.meta.settings) {
    csv += 'Settings\n';
    Object.entries(item.meta.settings).forEach(([k, v]) => {
      csv += `"${k}","${String(v).replace(/"/g, '""')}"\n`;
    });
    csv += '\n';
  }

  item.tables.forEach(table => {
    if (table.title) csv += `"${table.title.replace(/"/g, '""')}"\n`;
    const headers = table.columns.map(c => `"${c.label.replace(/"/g, '""')}"`).join(',');
    csv += headers + '\n';

    table.data.forEach(row => {
      const line = table.columns.map(col => {
        let val = row[col.key];
        if (val === null || val === undefined) return '';
        if (typeof val === 'string') return `"${val.replace(/"/g, '""')}"`;
        return val;
      }).join(',');
      csv += line + '\n';
    });
    csv += '\n';
  });

  return csv;
}
