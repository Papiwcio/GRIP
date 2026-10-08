"""Shared comparison-table presentation; no model or cell-value changes."""
from copy import deepcopy
import hashlib
from pathlib import Path
import re
from zipfile import ZipFile

from lxml import etree as ET

NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
REL = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
FONT_NAME, FONT_SIZE = 'Arial', 10
COEFFICIENT_HEIGHT, DEPENDENT_HEIGHT, HEADER_HEIGHT = 30, 42, 36
TARGETS = {
    'results_ols_scenarios.xlsx': {'Compare_Main': (2,10), 'Compare_Raw': (2,10)},
    'results_ols_interactions.xlsx': {'01_EXPORT_SIZE': (2,10), '02_PROFIT_MANUFACTURING': (2,10)},
    'results_quantile.xlsx': {f'Compare_{p}': (1,13) for p in ['FULL','P1','P2','P3']},
}


def format_comparison_table(writer, sheet_name, frame):
    """Apply the same styles to future native-engine report builds."""
    sheet = writer.sheets[sheet_name]
    sheet.hide_gridlines(2)
    body = writer.book.add_format({'font_name':FONT_NAME,'font_size':FONT_SIZE,'text_wrap':True,'valign':'vcenter'})
    numeric = writer.book.add_format({'font_name':FONT_NAME,'font_size':FONT_SIZE,'text_wrap':True,'valign':'vcenter','align':'center'})
    header = writer.book.add_format({'font_name':FONT_NAME,'font_size':FONT_SIZE,'bold':True,'bg_color':'#1F4E78','font_color':'white','text_wrap':True,'valign':'vcenter','align':'center'})
    identifiers = frame.columns.get_loc('display_name')+1
    for i,column in enumerate(frame.columns):
        width = 28 if i < identifiers-1 else 34 if i == identifiers-1 else 24
        sheet.set_column(i,i,width,body if i<identifiers else numeric)
        sheet.write(0,i,column,header)
    sheet.set_row(0,HEADER_HEIGHT)
    for number,row in enumerate(frame.itertuples(index=False,name=None),1):
        label = str(row[identifiers-1])
        height = DEPENDENT_HEIGHT if label.startswith('Dependent variable') else COEFFICIENT_HEIGHT if any(isinstance(v,str) and '\n(' in v for v in row[identifiers:]) else 18
        sheet.set_row(number,height)


def format_existing_workbook(path, specifications):
    """Patch only table presentation/styles; retain original namespaces/values."""
    from code_refresh_ols_exclusion_audit import validate_worksheet_xml
    path=Path(path)
    original=path.read_bytes()
    with ZipFile(path) as archive:
        infos=archive.infolist();parts={i.filename:archive.read(i.filename) for i in infos}
    styles=ET.fromstring(parts['xl/styles.xml'])
    fonts=styles.find(f'{{{NS}}}fonts');xfs=styles.find(f'{{{NS}}}cellXfs')
    font_cache, style_cache = {}, {}
    def fingerprint(element):
        return element.tag,tuple(sorted(element.attrib.items())),element.text,tuple(fingerprint(child) for child in element)
    known_fonts={fingerprint(font):i for i,font in enumerate(fonts)}
    known_styles={fingerprint(xf):i for i,xf in enumerate(xfs)}
    def style_for(old_id, alignment):
        key=(old_id,alignment)
        if key in style_cache:return style_cache[key]
        xf=deepcopy(xfs[old_id]);font_id=int(xf.get('fontId','0'))
        if font_id not in font_cache:
            font=deepcopy(fonts[font_id])
            font.find(f'{{{NS}}}name').set('val',FONT_NAME)
            font.find(f'{{{NS}}}sz').set('val',str(FONT_SIZE))
            scheme=font.find(f'{{{NS}}}scheme')
            if scheme is not None:font.remove(scheme)
            signature=fingerprint(font)
            if signature not in known_fonts:
                known_fonts[signature]=len(fonts);fonts.append(font)
            font_cache[font_id]=known_fonts[signature]
        xf.set('fontId',str(font_cache[font_id]));xf.set('applyFont','1')
        align=xf.find(f'{{{NS}}}alignment')
        if align is None:align=ET.SubElement(xf,f'{{{NS}}}alignment')
        align.set('wrapText','1');align.set('vertical','center');align.set('horizontal',alignment)
        xf.set('applyAlignment','1')
        signature=fingerprint(xf)
        if signature not in known_styles:
            known_styles[signature]=len(xfs);xfs.append(xf)
        style_cache[key]=known_styles[signature]
        return style_cache[key]
    book=ET.fromstring(parts['xl/workbook.xml'])
    relationships={r.get('Id'):r.get('Target') for r in ET.fromstring(parts['xl/_rels/workbook.xml.rels'])}
    sheets={s.get('name'):'xl/'+relationships[s.get(f'{{{REL}}}id')] for s in book.find(f'{{{NS}}}sheets')}
    strings=[''.join(s.itertext()) for s in ET.fromstring(parts['xl/sharedStrings.xml'])] if 'xl/sharedStrings.xml' in parts else []
    def text(cell):
        if cell.get('t')=='s':return strings[int(cell.find(f'{{{NS}}}v').text)]
        return ''.join(cell.find(f'{{{NS}}}is').itertext()) if cell.get('t')=='inlineStr' else ''
    changed={}
    counts={}
    for name,(identifiers,last) in specifications.items():
        root=validate_worksheet_xml(parts[sheets[name]])
        for view in root.findall(f'{{{NS}}}sheetViews/{{{NS}}}sheetView'):
            view.set('showGridLines','0')
        def values_signature(element):
            return [(cell.get('r'),cell.get('t'),[(entry.tag,entry.text,tuple(sorted(entry.attrib.items()))) for entry in cell.iter() if entry is not cell]) for cell in element.iter(f'{{{NS}}}c')]
        original_values=values_signature(root)
        count=0
        for row in root.find(f'{{{NS}}}sheetData'):
            number=int(row.get('r'))
            coefficient=False;dependent=False
            for cell in row:
                letters=re.match(r'[A-Z]+',cell.get('r')).group()
                column=0
                for letter in letters:column=column*26+ord(letter)-64
                if column>last:continue  # Researcher annotations stay intact.
                value=text(cell)
                if column==identifiers:dependent=value.startswith('Dependent variable')
                if column>identifiers and '\n(' in value:coefficient=True;count+=1
                cell.set('s',str(style_for(int(cell.get('s','0')),'left' if column<=identifiers and number!=1 else 'center')))
            height=HEADER_HEIGHT if number==1 else DEPENDENT_HEIGHT if dependent else COEFFICIENT_HEIGHT if coefficient else 18
            row.set('ht',str(max(float(row.get('ht','0')),height)));row.set('customHeight','1')
        columns=root.find(f'{{{NS}}}cols')
        old_columns=list(columns)
        for column in old_columns:columns.remove(column)
        for number in range(1,last+1):
            attrs=next((dict(c.attrib) for c in old_columns if int(c.get('min'))<=number<=int(c.get('max'))),{})
            attrs.update({'min':str(number),'max':str(number),'width':str(28 if number<identifiers else 34 if number==identifiers else 24),'customWidth':'1'})
            ET.SubElement(columns,f'{{{NS}}}col',attrs)
        for column in old_columns:
            if int(column.get('max'))>last:
                copy=deepcopy(column);copy.set('min',str(max(last+1,int(copy.get('min')))));columns.append(copy)
        changed[sheets[name]]=ET.tostring(root,encoding='UTF-8',xml_declaration=True,standalone=True)
        final_root=validate_worksheet_xml(changed[sheets[name]])
        assert values_signature(final_root)==original_values, f'Unexpected cell-value change: {name}'
        counts[name]=count
    fonts.set('count',str(len(fonts)));xfs.set('count',str(len(xfs)))
    changed['xl/styles.xml']=ET.tostring(styles,encoding='UTF-8',xml_declaration=True,standalone=True)
    temporary=path.with_name(path.stem+'.formatting.xlsx')
    with ZipFile(temporary,'w') as archive:
        for info in infos:archive.writestr(info,changed.get(info.filename,parts[info.filename]))
    with ZipFile(temporary) as archive:
        assert all(archive.read(name)==value for name,value in parts.items() if name not in changed)
    if hashlib.sha256(path.read_bytes()).digest()!=hashlib.sha256(original).digest():
        temporary.unlink();raise RuntimeError(f'Concurrent edit detected: {path}')
    temporary.replace(path)
    print(path.name,counts)
    return counts


if __name__=='__main__':
    for path,specifications in TARGETS.items():format_existing_workbook(path,specifications)
