// Presentation-only exporter. Audit calculations and review decisions are Python-owned.
import fs from 'node:fs/promises';
import { Workbook, SpreadsheetFile } from '@oai/artifact-tool';

const [input, destination, previewDirectory] = process.argv.slice(2);
const data = JSON.parse(await fs.readFile(input, 'utf8'));
const workbook = Workbook.create();
const sheetNames = ['README', 'Summary', 'Core_Findings', 'Period_Findings', 'Company_Review', 'Rule_Register'];
const letters = n => {
  let result = '';
  for (let x = n + 1; x > 0; x = Math.floor((x - 1) / 26)) result = String.fromCharCode(65 + (x - 1) % 26) + result;
  return result;
};
const safe = value => typeof value === 'string' && value.startsWith('=') ? "'" + value : value;

function table(sheet, records, columns, row, name, colour = '#1F4E78') {
  const last = letters(columns.length - 1);
  const matrix = [columns, ...records.map(record => columns.map(c => safe(record[c] ?? null)))];
  if (matrix.length + row > 1048576) throw new Error('Excel row limit exceeded; no findings silently truncated.');
  for (let start = 0; start < matrix.length; start += 2000) {
    const block = matrix.slice(start, start + 2000);
    sheet.getRange(`A${row + start}:${last}${row + start + block.length - 1}`).values = block;
  }
  const range = sheet.getRange(`A${row}:${last}${row + matrix.length - 1}`);
  range.format.font = { name: 'Arial', size: 10, color: '#222222' };
  range.format.verticalAlignment = 'center';
  range.format.rowHeight = 17;
  range.setNumberFormat('#,##0.0000;[Red](#,##0.0000);0');
  const head = sheet.getRange(`A${row}:${last}${row}`);
  head.format = { fill: colour, font: { name: 'Arial', size: 10, bold: true, color: '#FFFFFF' }, wrapText: true,
                  horizontalAlignment: 'center', verticalAlignment: 'center', rowHeight: 34 };
  const t = sheet.tables.add(`A${row}:${last}${row + Math.max(1, records.length)}`, true, name);
  t.style = 'TableStyleMedium2';
  t.showFilterButton = true;
  head.format.fill = colour;
  for (let i = 0; i < columns.length; i++) {
    const c = columns[i];
    const r = sheet.getRange(`${letters(i)}${row}:${letters(i)}${row + Math.max(1, records.length)}`);
    let width = 16;
    if (c === 'company') width = 62;
    if (['reason', 'description', 'formula_threshold_justification', 'limitation', 'reasons', 'affected_variables', 'source_amounts', 'related_values', 'lineage'].includes(c)) width = 65;
    if (['variable', 'category', 'manual_reason', 'location', 'severity_definition', 'implementation', 'analytically_relevant_findings'].includes(c)) width = 28;
    if (c === 'finding_id') width = 29;
    if (c === 'item') width = 42;
    r.format.columnWidth = width;
    if (['nip', 'company', 'period', 'finding_id', 'row_reference'].includes(c) || !['original_value','calculated_value'].includes(c) && records.some(x => typeof x[c] === 'string')) r.setNumberFormat('@');
    if (['year','review_rank','distinct_rules','findings','invalid_findings','analytically_relevant_findings','unresolved_high_priority','affected_companies','affected_firm_years','evaluated_contexts','not_evaluable_contexts'].includes(c)) r.setNumberFormat('#,##0');
    if (['year','source_year'].includes(c)) r.setNumberFormat('0');
    if (c === 'ratio_percent') r.setNumberFormat('0.0000"%"');
    if (['company', 'reason', 'description', 'formula_threshold_justification', 'limitation', 'reasons'].includes(c)) r.format.wrapText = true;
  }
  const severityIndex = columns.indexOf('severity');
  const classIndex = columns.indexOf('classification');
  if (records.length && severityIndex >= 0) {
    const target = sheet.getRange(`${letters(severityIndex)}${row + 1}:${letters(severityIndex)}${row + records.length}`);
    target.conditionalFormats.add('containsText', { text: 'critical', format: { fill: '#F4CCCC', font: { color: '#9C0006', bold: true } } });
    target.conditionalFormats.add('containsText', { text: 'high', format: { fill: '#FCE4D6', font: { color: '#9C4400', bold: true } } });
  }
  if (records.length && classIndex >= 0) {
    const target = sheet.getRange(`${letters(classIndex)}${row + 1}:${letters(classIndex)}${row + records.length}`);
    for (const [text, fill] of [['INVALID','#F4CCCC'],['SUSPICIOUS','#FFF2CC'],['EXTREME','#DDEBF7']]) {
      target.conditionalFormats.add('containsText', { text, format: { fill } });
    }
  }
  return row + matrix.length;
}

function populate(workbook, data) {
for (const name of sheetNames) {
  const sheet = workbook.worksheets.add(name);
  sheet.showGridLines = false;
  sheet.tabColor = name === 'README' ? '#808080' : name === 'Summary' ? '#1F4E78' : '#C65911';
  sheet.getRange('A2').values = [[name.replaceAll('_', ' ')]];
  sheet.getRange('A2').format.font = { name: 'Arial', size: 14, bold: true, color: '#1F4E78' };
  let records = data[name];
  const columns = name.endsWith('Findings') ? data.finding_columns : name === 'Summary' ? ['section','category','findings','affected_companies','affected_firm_years'] : records.length ? Object.keys(records[0]) : ['status'];
  const end = table(sheet, name === 'Summary' ? records.filter(r => r.section === 'Headlines') : records, columns, 4, `${name}Table`);
  sheet.freezePanes.freezeRows(4);
  if (['Core_Findings','Period_Findings'].includes(name)) sheet.freezePanes.freezeColumns(2);
  if (name === 'Company_Review') sheet.freezePanes.freezeColumns(3);
  if (name === 'Company_Review') sheet.getRange(`A5:${letters(columns.length - 1)}${Math.max(5,end - 1)}`).format.rowHeight = 34;
  if (name === 'README') {
    sheet.getRange(`A5:B${end - 1}`).format.rowHeight = 46;
    sheet.getRange(`B4:B${end - 1}`).format.columnWidth = 110;
  }
  if (name === 'Summary') {
    const share = records.findIndex(r => r.category === 'Affected firm share');
    if (share >= 0) sheet.getRange(`C${5 + share}`).setNumberFormat('0.00%');
    const topRow = end + 3;
    sheet.getRange(`A${topRow}`).values = [['20 companies requiring highest-priority review']];
    sheet.getRange(`A${topRow}`).format.font = { name: 'Arial', size: 12, bold: true };
    const topColumns = ['review_rank','company','nip','highest_severity','distinct_rules','analytically_relevant_findings','findings','rules'];
    table(sheet, data.Top20, topColumns, topRow + 2, 'Top20ReviewTable');
    const detailRow = topRow + data.Top20.length + 6;
    sheet.getRange(`A${detailRow}`).values = [['Findings by rule, classification, severity, year, sector and variable']];
    sheet.getRange(`A${detailRow}`).format.font = { name:'Arial', size:12, bold:true };
    table(sheet, records.filter(r => r.section !== 'Headlines'), columns, detailRow + 2, 'SummaryDetailsTable');
  }
  if (name === 'Rule_Register') sheet.getRange(`A5:J${end - 1}`).format.rowHeight = 105;
  if (name.endsWith('Findings')) sheet.getRange(`A5:${letters(columns.length - 1)}${Math.max(5,end - 1)}`).format.rowHeight = 34;
}
}
populate(workbook, data);
workbook.recalculate();
console.log((await workbook.inspect({kind:'table', range:'Summary!A4:E12', include:'values', tableMaxRows:9, tableMaxCols:5, maxChars:2500})).ndjson);
const errors = await workbook.inspect({kind:'match', searchTerm:'#REF!|#DIV/0!|#VALUE!|#NAME\\?|#NUM!|#SPILL!', options:{useRegex:true,maxResults:20}, maxChars:2000});
console.log(errors.ndjson);
if (previewDirectory) {
  await fs.mkdir(previewDirectory, { recursive: true });
  // Rendering clones a workbook into a worker. Preserve identical layout/style but
  // inspect the visible leading ranges in a small workbook, not 1M+ off-screen cells.
  const previewData = {...data};
  for (const name of sheetNames) previewData[name] = data[name].slice(0, 12);
  const previewWorkbook = Workbook.create();
  populate(previewWorkbook, previewData);
  previewWorkbook.recalculate();
  for (const name of sheetNames) {
    const range = name === 'README' ? 'A1:B12' : name.endsWith('Findings') ? 'A1:M11' : name === 'Rule_Register' ? 'A1:F7' : 'A1:H11';
    const preview = await previewWorkbook.render({sheetName:name, range, scale:1.2, format:'png'});
    await fs.writeFile(`${previewDirectory}/${name}.png`, new Uint8Array(await preview.arrayBuffer()));
  }
}
if (!destination) process.exit(0); // Bounded previews only; full-size exporter is Python.
await fs.mkdir(destination.substring(0, destination.lastIndexOf('/')), { recursive:true });
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(destination);
console.log(`Audit workbook exported: ${destination}`);
