"""Retained Excel namespace checks plus current consolidated exclusion validation."""
import unittest
import code_refresh_ols_exclusion_audit as audit
from lxml import etree as ET

class ConsolidatedTests(unittest.TestCase):
    def test_rejects_excel_compatibility_prefix_loss(self):
        # Well-formed XML can still be rejected by Excel when mc:Ignorable
        # references undeclared namespaces. The original repair escaped parser
        # and numerical checks; reproduce that precise failure here.
        invalid=f'<worksheet xmlns="{audit.NS}" xmlns:mc="{audit.MC}" mc:Ignorable="x14ac xr2"><sheetData/></worksheet>'
        ET.fromstring(invalid.encode())  # Ordinary XML parsing wrongly passes.
        with self.assertRaisesRegex(ValueError,'Undeclared Excel compatibility prefix'):
            audit.validate_worksheet_xml(invalid.encode())

    def test_preserves_unused_qname_namespace_declarations(self):
        xml=f'<worksheet xmlns="{audit.NS}" xmlns:mc="{audit.MC}" xmlns:xr2="http://schemas.microsoft.com/office/spreadsheetml/2015/revision2" mc:Ignorable="xr2"><dimension ref="A1:B3"/><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>item</t></is></c><c r="B1" t="inlineStr"><is><t>description</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>Purpose</t></is></c><c r="B2" t="inlineStr"><is><t>Test</t></is></c></row><row r="3"><c r="A3" t="inlineStr"><is><t>Manual exclusions</t></is></c><c r="B3" t="inlineStr"><is><t>Test</t></is></c></row></sheetData></worksheet>'
        updated=audit.insert_readme_note(xml.encode(),[], 'Full exclusion audit.')
        root=audit.validate_worksheet_xml(updated)
        self.assertIn('xr2',root.nsmap)
        self.assertEqual(root.get(f'{{{audit.MC}}}Ignorable'),'xr2')

    def test_central_exclusion_registers_and_alignment(self):
        from code_common_samples import current,ROOT
        from code_check_common_samples import validate_context,validate_workbooks
        context=current();validate_context(context);validate_workbooks(ROOT,context)

if __name__=='__main__': unittest.main(verbosity=2)
