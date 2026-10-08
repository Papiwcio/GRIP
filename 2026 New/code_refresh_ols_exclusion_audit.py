"""Refresh only the primary OLS exclusion audit; retain every other Excel part.

Use the existing shared sample/missing-value rules without estimating regressions.
Native worksheet XML replacement preserves researcher formatting/annotations on
unrelated sheets and avoids republishing any regression or diagnostic results.
"""
from contextlib import redirect_stdout
import hashlib
import io
from pathlib import Path
import re
from lxml import etree as ET
from zipfile import ZipFile

import numpy as np
import pandas as pd
import code_ols_scenarios as engine

NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
# Preserve the source namespace map. ElementTree drops declarations that occur
# only inside mc:Ignorable/QName attribute VALUES; Excel then discards the sheet.
MC = "http://schemas.openxmlformats.org/markup-compatibility/2006"


def validate_worksheet_xml(xml):
    root = ET.fromstring(xml)
    for element in root.iter():
        for attribute, value in element.attrib.items():
            if attribute == f"{{{MC}}}Ignorable":
                for prefix in value.split():
                    if prefix not in element.nsmap:
                        raise ValueError(f"Undeclared Excel compatibility prefix: {prefix}")
            elif attribute in {f"{{{MC}}}MustUnderstand", f"{{{MC}}}ProcessContent", f"{{{MC}}}PreserveAttributes", f"{{{MC}}}PreserveElements"}:
                for token in value.split():
                    prefix = token.split(":")[0]
                    if prefix not in element.nsmap:
                        raise ValueError(f"Undeclared Excel compatibility QName: {token}")
    if root.find(f"{{{NS}}}sheetData") is None:
        raise ValueError("Missing worksheet data element.")
    return root


def build_audit(config=engine.CONFIG):
    config = engine.normalise_config({**config, "include_interactions": False})
    models = engine.build_models(config)
    source = pd.read_parquet(config["input_file"])
    with redirect_stdout(io.StringIO()):
        data = engine.load_input_data(config)
    results, drops, registry = {}, [], {}
    for scenario, query in engine.SCENARIOS.items():
        local = {**config, "sample_name": engine.SHARED_SAMPLE_SCENARIOS.get(scenario, scenario), "base_sample_filter": "True"}
        scenario_df = engine.apply_scenario_filter(data, scenario, query)
        frame = engine.apply_sample_filter(scenario_df, local)
        frame = engine.add_interaction_columns(frame, local, models)
        results[scenario] = {"filtered_df": frame}
        drops.append(engine.add_scenario_column(engine.build_dropped_rows_table(frame, local, models), scenario))
        registry.update(engine.build_variable_registry(local, models, engine.get_categorical_levels(frame, local)))
    early = engine.build_pre_model_exclusion_table(source, results, config)
    table = pd.concat([early, *drops], ignore_index=True)
    registry = engine.add_dependent_variables_to_registry(registry)
    return engine.add_dropped_row_labels(table, registry), config, len(source)


def validate_reconciliation(table, summary, source_n):
    early = table.loc[table.exclusion_stage.ne("model_complete_case")]
    if early.duplicated(["scenario", "nip"]).any():
        raise AssertionError("An early exclusion must appear once per firm/scenario.")
    for row in summary.itertuples():
        before = early.scenario.eq(row.scenario).sum()
        missing = (table.scenario.eq(row.scenario) & table.model.eq(row.model) & table.exclusion_stage.eq("model_complete_case")).sum()
        if source_n != before + missing + row.observations:
            raise AssertionError(f"Exclusion counts do not reconcile for {row.scenario}/{row.model}.")


def col_letter(number):
    text = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        text = chr(65 + remainder) + text
    return text


def add_cell(row, reference, value, style=None):
    attributes = {"r": reference}
    if style is not None:
        attributes["s"] = style
    cell = ET.SubElement(row, f"{{{NS}}}c", attributes)
    if pd.isna(value):
        return
    if isinstance(value, (int, float, np.integer, np.floating)) and np.isfinite(value):
        ET.SubElement(cell, f"{{{NS}}}v").text = str(value)
    else:
        cell.set("t", "inlineStr")
        inline = ET.SubElement(cell, f"{{{NS}}}is")
        ET.SubElement(inline, f"{{{NS}}}t").text = str(value)


def replace_audit_sheet(xml, table):
    root = validate_worksheet_xml(xml)
    data = root.find(f"{{{NS}}}sheetData")
    old = list(data)
    header_style = old[0][0].get("s")
    styles = {re.sub(r"\d", "", cell.get("r")): cell.get("s") for cell in old[1]}
    cols = root.find(f"{{{NS}}}cols")
    wrap_style = styles.get("K") or next((col.get("style") for col in cols if int(col.get("min")) <= 11 <= int(col.get("max"))), None)
    data.clear()
    header = ET.SubElement(data, f"{{{NS}}}row", {"r": "1", "ht": "32", "customHeight": "1"})
    for i, name in enumerate(table.columns, 1):
        add_cell(header, f"{col_letter(i)}1", name, header_style)
    for number, values in enumerate(table.itertuples(index=False, name=None), 2):
        early = values[table.columns.get_loc("exclusion_stage")] != "model_complete_case"
        row = ET.SubElement(data, f"{{{NS}}}row", {"r": str(number)})
        if early:
            row.set("ht", "90"); row.set("customHeight", "1")
        for i, value in enumerate(values, 1):
            style = wrap_style if table.columns[i-1] in {"reason", "scope", "trajectory_flag", "exclusion_stage", "reason_code"} else styles.get(col_letter(i))
            add_cell(row, f"{col_letter(i)}{number}", value, style)
    extent = f"A1:{col_letter(len(table.columns))}{len(table)+1}"
    root.find(f"{{{NS}}}dimension").set("ref", extent)
    root.find(f"{{{NS}}}autoFilter").set("ref", extent)
    # Existing generated extra-column spans are replaced on repeat refreshes.
    for column in list(cols):
        if int(column.get("min")) > 15:
            cols.remove(column)
        elif int(column.get("min")) <= 15 <= int(column.get("max")):
            column.set("max", "15")
    for column in cols:
        if int(column.get("min")) <= 15 <= int(column.get("max")):
            # Split the final existing span so the explanatory reason is wide.
            if int(column.get("min")) < 15:
                column.set("max", "14")
            else:
                cols.remove(column)
            break
    ET.SubElement(cols, f"{{{NS}}}col", {"min": "15", "max": "15", "width": "60", "customWidth": "1", **({"style": wrap_style} if wrap_style else {})})
    for i, name in enumerate(table.columns, 1):
        if i > 15:
            ET.SubElement(cols, f"{{{NS}}}col", {"min": str(i), "max": str(i), "width": "34", "customWidth": "1"})
    output = ET.tostring(root, encoding="UTF-8", xml_declaration=True, standalone=True)
    validate_worksheet_xml(output)
    return output


def insert_readme_note(xml, strings, description):
    root = validate_worksheet_xml(xml)
    data = root.find(f"{{{NS}}}sheetData")
    def text(cell):
        value = cell.find(f"{{{NS}}}v")
        if cell.get("t") == "s": return strings[int(value.text)]
        return "".join(cell.itertext())
    existing = list(data)
    audit_row = next((row for row in existing if text(row[0]) == "Dropped-firm audit"), None)
    if audit_row is not None:
        insertion = int(audit_row.get("r"))
        data.remove(audit_row)
    else:
        insertion = next((int(row.get("r")) for row in existing if text(row[0]) == "Manual exclusions"), len(existing)+1)
        for row in existing:
            if int(row.get("r")) >= insertion:
                number = int(row.get("r")) + 1
                row.set("r", str(number))
                for cell in row: cell.set("r", re.sub(r"\d+", str(number), cell.get("r")))
    template = existing[1]
    new = ET.Element(f"{{{NS}}}row", {"r": str(insertion), "ht": "126", "customHeight": "1"})
    add_cell(new, f"A{insertion}", "Dropped-firm audit", template[0].get("s"))
    add_cell(new, f"B{insertion}", description, template[1].get("s"))
    data.insert(insertion-1, new)
    root.find(f"{{{NS}}}dimension").set("ref", f"A1:B{max(int(row.get('r')) for row in data)}")
    output = ET.tostring(root, encoding="UTF-8", xml_declaration=True, standalone=True)
    validate_worksheet_xml(output)
    return output


def refresh(destination="results_ols_scenarios.xlsx"):
    table, config, source_n = build_audit()
    summary = pd.read_excel(destination, sheet_name="Model_Summary_Long")
    validate_reconciliation(table, summary, source_n)
    old_drops = pd.read_excel(destination, sheet_name="Dropped_Rows_Long")
    if "exclusion_stage" in old_drops:
        old_drops = old_drops.loc[old_drops.exclusion_stage.eq("model_complete_case")]
    existing_columns = [column for column in old_drops if column not in {"exclusion_stage", "reason_code", "scope", "trajectory_flag", "trajectory_flag_value", "in_rank_2019", "manufacturing"}]
    current = table.loc[table.exclusion_stage.eq("model_complete_case"), existing_columns]
    pd.testing.assert_frame_equal(old_drops[existing_columns].reset_index(drop=True).fillna("").astype(str), current.reset_index(drop=True).fillna("").astype(str), check_dtype=False)
    path = Path(destination)
    before_hash = hashlib.sha256(path.read_bytes()).digest()
    with ZipFile(path) as old:
        parts = {info.filename: old.read(info.filename) for info in old.infolist()}
        infos = old.infolist()
    book = ET.fromstring(parts["xl/workbook.xml"])
    relations = {element.get("Id"): element.get("Target") for element in ET.fromstring(parts["xl/_rels/workbook.xml.rels"])}
    sheets = {sheet.get("name"): "xl/" + relations[sheet.get(f"{{{REL}}}id")] for sheet in book.find(f"{{{NS}}}sheets")}
    strings = ["".join(entry.itertext()) for entry in ET.fromstring(parts["xl/sharedStrings.xml"])]
    changed = {
        sheets["Dropped_Rows_Long"]: replace_audit_sheet(parts[sheets["Dropped_Rows_Long"]], table),
        sheets["README"]: insert_readme_note(parts[sheets["README"]], strings, engine.build_readme_sheet(config).set_index("item").loc["Dropped-firm audit", "description"]),
    }
    temporary = path.with_name(path.stem + ".audit-writing.xlsx")
    with ZipFile(temporary, "w") as out:
        for info in infos: out.writestr(info, changed.get(info.filename, parts[info.filename]))
    with ZipFile(temporary) as check:
        assert all(check.read(name) == value for name, value in parts.items() if name not in changed)
        for name in changed:
            validate_worksheet_xml(check.read(name))
    if hashlib.sha256(path.read_bytes()).digest() != before_hash:
        temporary.unlink()
        raise RuntimeError("Workbook changed concurrently; update not published.")
    temporary.replace(path)
    print(table.groupby(["scenario", "exclusion_stage"]).size().to_string())
    print(f"All {len(summary)} model counts reconcile to {source_n} original firms. Only README and Dropped_Rows_Long changed.")
    return table


if __name__ == "__main__":
    refresh()
