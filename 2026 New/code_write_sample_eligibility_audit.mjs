// Separate audit presentation only. No production workbooks or source data writes.
import fs from 'node:fs/promises';
import { Workbook, SpreadsheetFile } from '@oai/artifact-tool';

const [input, output, previewDir] = process.argv.slice(2);
if (!input || !output) throw new Error('Expected payload JSON, audit XLSX destination, optional preview directory.');
if (!output.endsWith('/results_sample_eligibility_audit.xlsx')) throw new Error('Only the separate audit workbook may be written.');
const data = JSON.parse(await fs.readFile(input, 'utf8'));
const letters = n => {
  let result = '';
  for (let x = n + 1; x > 0; x = Math.floor((x - 1) / 26)) result = String.fromCharCode(65 + (x - 1) % 26) + result;
  return result;
};
const columns = {
  '01_SAMPLE_COUNTS': ['scenario','period','current_N','valid_N','newly_excluded_N','common_sample_N','additional_common_loss_N','additional_common_loss_share','specification','current_four_period_intersection_N','interaction_extra_missing_N','variant_company_sets_identical','current_ID_SHA256','valid_ID_SHA256'],
  '02_SAMPLE_OVERLAP': ['specification','scenario','state','periods','unique_firms','union_N','intersection_ID_SHA256'],
  '03_EXCLUSION_REGISTER': ['specification','scenario','nip','company','period','variable','observed_value','source_year','numerator','denominator','rule','classification','exclusion_status','currently_in_regression','enters_any_current_regression','in_trajectory_diagnostics','in_regression_diagnostics','in_correlations','reason'],
  '04_DIAGNOSTICS_ALIGNMENT': ['row_type','scenario','period','diagnostic_scope','diagnostic_N','regression_N','intersection_N','diagnostics_only_N','regression_only_N','nip','company','variable','observed_value','reason'],
};
const titles = {
  '01_SAMPLE_COUNTS': 'Sample eligibility by scenario and period',
  '02_SAMPLE_OVERLAP': 'Company intersections across periods',
  '03_EXCLUSION_REGISTER': 'Existing exclusions and proposed validity findings',
  '04_DIAGNOSTICS_ALIGNMENT': 'Diagnostic and regression sample alignment',
};
const notes = {
  '01_SAMPLE_COUNTS': 'Audit only. Common sample = intersection of proposed-valid P1, P2, P3 and FULL company IDs.',
  '02_SAMPLE_OVERLAP': 'Unique NIPs. Exact membership patterns use the order P1 / P2 / P3 / FULL.',
  '03_EXCLUSION_REGISTER': 'CONDITIONAL exports: proposed [0,1] eligibility. Reporting boundaries remain unresolved. No source corrections.',
  '04_DIAGNOSTICS_ALIGNMENT': 'SUMMARY rows give counts. COMPANY rows identify diagnostics-only firms. Missingness and trajectory scope differ from complete cases.',
};
const headerLabel = c => ({
  current_N:'Current N',valid_N:'Valid N',newly_excluded_N:'New exclusions N',
  common_sample_N:'Common sample N',additional_common_loss_N:'Additional common loss N',
  additional_common_loss_share:'Additional common loss %',
  current_four_period_intersection_N:'Current four-period intersection N',
  interaction_extra_missing_N:'Additional interaction missingness N',
}[c] ?? c.replaceAll('_',' '));

function populate(workbook, limit = null) {
  for (const [name, cols] of Object.entries(columns)) {
    const sheet = workbook.worksheets.add(name);
    sheet.showGridLines = false;
    sheet.tabColor = name === '01_SAMPLE_COUNTS' ? '#1F4E78' : '#808080';
    sheet.getRange('A2').values = [[titles[name]]];
    sheet.getRange('A2').format.font = { name: 'Arial', size: 14, bold: true, color: '#1F4E78' };
    sheet.getRange('A3').values = [[notes[name]]];
    sheet.getRange('A3').format.font = { name: 'Arial', size: 10, italic: true, color: '#595959' };
    const ordered = [...data.tables[name]];
    if (name === '04_DIAGNOSTICS_ALIGNMENT') ordered.sort((a,b) => (a.row_type === 'SUMMARY' ? 0 : 1) - (b.row_type === 'SUMMARY' ? 0 : 1));
    if (name === '03_EXCLUSION_REGISTER') {
      const order = {NEWLY_PROPOSED:0,SOURCE_SCREEN_ONLY:1,ALREADY_EXCLUDED:2,EXISTING:3};
      ordered.sort((a,b) => order[a.exclusion_status] - order[b.exclusion_status]);
    }
    const records = limit ? ordered.slice(0, limit) : ordered;
    const last = letters(cols.length - 1), end = 5 + records.length;
    const matrix = [cols.map(headerLabel), ...records.map(r => cols.map(c => r[c] ?? null))];
    for (let start = 0; start < matrix.length; start += 1000) {
      const block = matrix.slice(start, start + 1000);
      sheet.getRange(`A${5 + start}:${last}${4 + start + block.length}`).values = block;
    }
    const range = sheet.getRange(`A5:${last}${end}`);
    range.format.font = { name: 'Arial', size: 10, color: '#222222' };
    range.format.verticalAlignment = 'center';
    range.format.rowHeight = name === '03_EXCLUSION_REGISTER' ? 30 : 26;
    const table = sheet.tables.add(`A5:${last}${end}`, true, `Audit${name.slice(0,2)}`);
    table.style = 'TableStyleMedium2';
    table.showFilterButton = true;
    for (let i = 0; i < cols.length; i++) {
      const c = cols[i], col = sheet.getRange(`${letters(i)}5:${letters(i)}${end}`);
      col.format.columnWidth = c === 'scenario' ? 34 : c === 'period' ? 10 : c === 'company' ? 58 : c === 'reason' ? 75 : c === 'diagnostic_scope' ? 64 :
        c === 'periods' ? 53 : c.includes('SHA256') ? 70 : ['variable','rule','specification'].includes(c) ? 32 :
        c.includes('share') ? 20 : c.includes('N') ? 18 : c.includes('regression') || c.includes('diagnostics') || c.includes('correlations') ? 24 : 23;
      col.setNumberFormat(c.includes('share') ? '0.00%' : c === 'observed_value' ? '0.00000000;[Red](0.00000000);0' : ['numerator','denominator'].includes(c) ? '#,##0.0000;[Red](#,##0.0000);0' : c === 'source_year' ? '0' : c.endsWith('_N') || ['unique_firms','union_N'].includes(c) ? '#,##0' : '@');
      if (records.some(r => typeof r[c] === 'number')) col.format.horizontalAlignment = 'right';
      else col.format.horizontalAlignment = 'left';
      if (['company','reason','diagnostic_scope','variable'].includes(c)) col.format.wrapText = true;
    }
    sheet.getRange(`A5:${last}5`).format = {
      fill: '#1F4E78', font: { name: 'Arial', size: 10, bold: true, color: '#FFFFFF' },
      horizontalAlignment: 'center', verticalAlignment: 'center', wrapText: true, rowHeight: 48,
    };
    if (name === '03_EXCLUSION_REGISTER') {
      for (const [text, fill] of [['NEWLY_PROPOSED','#FFF2CC'],['ALREADY_EXCLUDED','#E7E6E6']])
        sheet.getRange(`M6:M${end}`).conditionalFormats.add('containsText', {text, format: {fill}});
    }
    sheet.freezePanes.freezeRows(5);
    sheet.freezePanes.freezeColumns(name === '03_EXCLUSION_REGISTER' ? 3 : 2);
  }
}
const workbook = Workbook.create();
populate(workbook);
workbook.recalculate();
console.log((await workbook.inspect({kind:'table', range:'01_SAMPLE_COUNTS!A5:I9', include:'values', tableMaxRows:5, tableMaxCols:9, maxChars:2000})).ndjson);
console.log((await workbook.inspect({kind:'match', searchTerm:'#REF!|#DIV/0!|#VALUE!|#NAME\\?|#NUM!|#SPILL!', options:{useRegex:true,maxResults:10}, maxChars:1000})).ndjson);
await (await SpreadsheetFile.exportXlsx(workbook)).save(output);
// Keep the exporter-generated inspection log with temporary support artifacts.
if (previewDir) {
  await fs.mkdir(previewDir, {recursive:true});
  await fs.rename(`${output}.inspect.ndjson`, `${previewDir}/audit_export.inspect.ndjson`).catch(e => { if (e.code !== 'ENOENT') throw e; });
}
if (previewDir) {
  await fs.mkdir(previewDir, {recursive:true});
  const preview = Workbook.create();
  populate(preview, 12);
  preview.recalculate();
  for (const name of Object.keys(columns)) {
    const endcol = name === '03_EXCLUSION_REGISTER' ? 'K' : name === '04_DIAGNOSTICS_ALIGNMENT' ? 'I' : name === '02_SAMPLE_OVERLAP' ? 'F' : 'I';
    const blob = await preview.render({sheetName:name, range:`A1:${endcol}11`, scale:1.2, format:'png'});
    await fs.writeFile(`${previewDir}/${name}.png`, new Uint8Array(await blob.arrayBuffer()));
  }
}
console.log(`Exported four audit sheets: ${output}`);
