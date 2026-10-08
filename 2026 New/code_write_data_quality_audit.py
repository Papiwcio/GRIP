"""Large-table XLSX export fallback after artifact-tool exceeded report memory limits.

Presentation only: no financial calculations, exclusions or source writes.
Native Excel tables require normal XlsxWriter mode (constant_memory disallows tables).
"""
from pathlib import Path
import xlsxwriter


def write_audit_workbook(payload, destination):
    destination = Path(destination)
    temporary = destination.with_name(destination.stem + ".writing.xlsx")
    with xlsxwriter.Workbook(temporary) as workbook:
        workbook.set_properties({"title": "GRIP data-quality audit", "comments": "Reporting only; source files unchanged."})
        base = {"font_name": "Arial", "font_size": 10, "valign": "vcenter"}
        text = workbook.add_format({**base, "text_wrap": True})
        number = workbook.add_format({**base, "num_format": "#,##0.0000;[Red](#,##0.0000);0"})
        integer = workbook.add_format({**base, "num_format": "#,##0"})
        year = workbook.add_format({**base, "num_format": "0"})
        percent_points = workbook.add_format({**base, "num_format": '0.0000"%"'})
        percent = workbook.add_format({**base, "num_format": "0.00%"})
        title = workbook.add_format({**base, "font_size": 14, "bold": True, "font_color": "#1F4E78"})
        header = workbook.add_format({**base, "bold": True, "font_color": "white", "bg_color": "#1F4E78", "text_wrap": True, "align": "center"})
        critical = workbook.add_format({"bg_color": "#F4CCCC", "font_color": "#9C0006", "bold": True})
        high = workbook.add_format({"bg_color": "#FCE4D6", "font_color": "#9C4400", "bold": True})
        suspect = workbook.add_format({"bg_color": "#FFF2CC"})
        extreme = workbook.add_format({"bg_color": "#DDEBF7"})
        count_columns = {"review_rank", "distinct_rules", "findings", "invalid_findings", "analytically_relevant_findings", "unresolved_high_priority", "affected_companies", "affected_firm_years", "evaluated_contexts", "not_evaluable_contexts"}

        def table(sheet, records, columns, start, table_name):
            if start + len(records) + 1 > 1048576:
                raise ValueError("Excel row limit exceeded; no finding truncated.")
            formats = []
            for col, name in enumerate(columns):
                fmt = year if name in {"year", "source_year"} else integer if name in count_columns else percent_points if name == "ratio_percent" else number if name in {"original_value", "calculated_value", "numerator", "denominator", "sales", "exports", "net_profit", "employment", "total_assets", "equity", "fixed_assets", "current_assets", "wages_total", "operating_result"} else text
                formats.append(fmt)
                width = 16
                if name == "company": width = 62
                if name in {"reason", "description", "formula_threshold_justification", "limitation", "reasons", "affected_variables", "source_amounts", "related_values", "lineage"}: width = 65
                if name in {"variable", "category", "manual_reason", "location", "severity_definition", "implementation", "analytically_relevant_findings"}: width = 28
                if name == "finding_id": width = 29
                if name == "item": width = 42
                if sheet.name == "README" and name == "description": width = 110
                sheet.set_column(col, col, width, fmt)
            for offset, record in enumerate(records, start + 1):
                for col, name in enumerate(columns):
                    value = record.get(name)
                    if value is None: continue
                    if isinstance(value, str):
                        if len(value) > 32767:
                            raise ValueError(f"Cell exceeds Excel text limit: {sheet.name}/{name}. No text silently truncated.")
                        sheet.write_string(offset, col, value, formats[col])
                    elif isinstance(value, bool): sheet.write_boolean(offset, col, value, formats[col])
                    else: sheet.write_number(offset, col, value, percent if record.get("category") == "Affected firm share" and name == "findings" else formats[col])
                height = 46 if sheet.name == "README" else 105 if sheet.name == "Rule_Register" else 34 if sheet.name in {"Core_Findings", "Period_Findings", "Company_Review"} else 20
                sheet.set_row(offset, height)
            last = start + max(1, len(records))
            sheet.add_table(start, 0, last, len(columns) - 1, {"name": table_name, "style": "Table Style Medium 2", "columns": [{"header": name, "header_format": header} for name in columns], "autofilter": True})
            sheet.set_row(start, 34)
            if "severity" in columns and records:
                col = columns.index("severity")
                for value, fmt in [("critical", critical), ("high", high)]:
                    sheet.conditional_format(start + 1, col, last, col, {"type": "text", "criteria": "containing", "value": value, "format": fmt})
            if "classification" in columns and records:
                col = columns.index("classification")
                for value, fmt in [("INVALID", critical), ("SUSPICIOUS", suspect), ("EXTREME", extreme)]:
                    sheet.conditional_format(start + 1, col, last, col, {"type": "text", "criteria": "containing", "value": value, "format": fmt})
            return last

        for name in ["README", "Summary", "Core_Findings", "Period_Findings", "Company_Review", "Rule_Register"]:
            sheet = workbook.add_worksheet(name)
            sheet.hide_gridlines(2)
            sheet.set_tab_color("#808080" if name == "README" else "#1F4E78" if name == "Summary" else "#C65911")
            sheet.write(1, 0, name.replace("_", " "), title)
            records = payload[name]
            columns = payload["finding_columns"] if name.endswith("Findings") else ["section", "category", "findings", "affected_companies", "affected_firm_years"] if name == "Summary" else list(records[0]) if records else ["status"]
            last = table(sheet, [r for r in records if r["section"] == "Headlines"] if name == "Summary" else records, columns, 3, name + "Table")
            sheet.freeze_panes(4, 2 if name.endswith("Findings") else 3 if name == "Company_Review" else 0)
            if name == "Summary":
                start = last + 3
                sheet.write(start, 0, "20 companies requiring highest-priority review", title)
                cols = ["review_rank", "company", "nip", "highest_severity", "distinct_rules", "analytically_relevant_findings", "findings", "rules"]
                last = table(sheet, payload["Top20"], cols, start + 2, "Top20ReviewTable")
                start = last + 3
                sheet.write(start, 0, "Findings by rule, classification, severity, year, sector and variable", title)
                table(sheet, [r for r in records if r["section"] != "Headlines"], columns, start + 2, "SummaryDetailsTable")
    temporary.replace(destination)
