#!/usr/bin/env python3
"""
XMLLog_SubtestInventory_v01.py

Lightweight CONTENT/ITEM subtest inventory scanner for Sakura XML logs.

Purpose
-------
This diagnostic script scans XML files only far enough to read the CONTENT/ITEM
subtest list. It does not parse full SUBTEST data, waveform data, emissions data,
or PAR values. It is intended to quickly characterize subtest naming patterns
across a large XML archive.

Output workbook sheets
----------------------
1. Inventory
   One row per CONTENT/ITEM entry.

2. File_Summary
   One row per XML file with file size, bytes read, item count, LS/M summary,
   and max observed LS/M indices.

3. Pattern_Summary
   Counts by SubtestID and SubtestType.

4. Unexpected_Patterns
   CONTENT/ITEM labels that could not be parsed cleanly.

Example
-------
python XMLLog_SubtestInventory_v01.py "\\\\vcn.ds.volvo.net\\vpt-hag\\proj11\\026298\\Pool\\HG52\\2026" --recursive --out HG52_SubtestInventory.xlsx

Notes
-----
- This script assumes CONTENT/ITEM appears before the heavy waveform sections.
- It stops scanning each XML after the end of CONTENT is reached.
- It is intentionally much lighter than XMLLog_DirectParser_v04_Phase2A_MP_fixed.py.
"""

from __future__ import annotations

import argparse
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
import xml.etree.ElementTree as ET

try:
    import pandas as pd
except Exception as exc:
    raise SystemExit("ERROR: pandas is required. Install with: python -m pip install pandas openpyxl") from exc

try:
    import openpyxl  # noqa: F401
except Exception as exc:
    raise SystemExit("ERROR: openpyxl is required. Install with: python -m pip install openpyxl") from exc

XML_NAME_RE = re.compile(r"^(HG\d{2})_(\d{4})-\d{2}-\d{2}_\d+", re.IGNORECASE)
TOKEN_RE = re.compile(r"^(?P<num>\d+)(?P<id>[A-Za-z]+\d*)(?:\s+(?P<type>.*))?$")
M_ID_RE = re.compile(r"^M(?P<mnum>\d+)$", re.IGNORECASE)
LS_ID_RE = re.compile(r"^LS$", re.IGNORECASE)


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def infer_cell(path: Path) -> str:
    m = XML_NAME_RE.match(path.name)
    if m:
        return m.group(1).upper()
    for part in path.parts:
        if re.fullmatch(r"HG\d+", part, flags=re.IGNORECASE):
            return part.upper()
    return ""


def infer_year(path: Path) -> str:
    m = XML_NAME_RE.match(path.name)
    if m:
        return m.group(2)
    for part in reversed(path.parts):
        if re.fullmatch(r"(?:19|20)\d{2}", part):
            return part
    return ""


def parse_content_item(item: str) -> dict[str, Any]:
    """Parse labels such as '0LS Calibration', '1M1 FullLoad', or '999M1 Special'."""
    item = clean_text(item)
    result = {
        "ContentItem": item,
        "Token": "",
        "SubtestNum": "",
        "SubtestID": "",
        "SubtestType": "",
        "IsLS": False,
        "IsM": False,
        "Is999": False,
        "MNumber": "",
        "ParseStatus": "Parsed",
        "ParseNote": "",
    }

    if not item:
        result["ParseStatus"] = "Blank"
        result["ParseNote"] = "Empty CONTENT/ITEM text"
        return result

    parts = item.split(maxsplit=1)
    token = parts[0]
    stype = parts[1] if len(parts) > 1 else ""

    result["Token"] = token
    result["SubtestType"] = stype

    m = TOKEN_RE.match(item)
    if not m:
        result["ParseStatus"] = "Unparsed"
        result["ParseNote"] = "Does not match numeric-prefix token pattern"
        return result

    num = m.group("num") or ""
    sid = m.group("id") or ""
    parsed_type = m.group("type") or ""

    result["SubtestNum"] = num
    result["SubtestID"] = sid
    result["SubtestType"] = parsed_type
    result["Is999"] = num == "999"
    result["IsLS"] = bool(LS_ID_RE.match(sid))

    mm = M_ID_RE.match(sid)
    if mm:
        result["IsM"] = True
        result["MNumber"] = mm.group("mnum")

    if not result["IsLS"] and not result["IsM"]:
        result["ParseStatus"] = "ParsedOther"
        result["ParseNote"] = "Parsed but SubtestID is neither LS nor M#"

    return result


def find_input_xmls(inputs: list[str], recursive: bool, max_files: Optional[int]) -> list[Path]:
    files: list[Path] = []
    for item in inputs:
        p = Path(item)
        if p.is_file() and p.suffix.lower() == ".xml":
            files.append(p)
        elif p.is_dir():
            files.extend(p.glob("**/*.xml" if recursive else "*.xml"))
        else:
            print(f"WARNING: input not found or not XML: {p}")
    files = sorted(set(files), key=lambda x: str(x).lower())
    if max_files is not None:
        files = files[:max_files]
    return files


class CountingReader:
    def __init__(self, fh):
        self.fh = fh
        self.bytes_read = 0

    def read(self, size=-1):
        data = self.fh.read(size)
        self.bytes_read += len(data)
        return data

    def close(self):
        return self.fh.close()


def inventory_file(xml_file: Path) -> tuple[list[dict[str, Any]], dict[str, Any], Optional[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    t0 = time.perf_counter()

    try:
        stat = xml_file.stat()
        file_size = stat.st_size
        modified = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        file_size = 0
        modified = ""

    cell = infer_cell(xml_file)
    year = infer_year(xml_file)
    bytes_read = 0
    content_started = False
    content_done = False
    content_item_count = 0

    try:
        with xml_file.open("rb") as raw_fh:
            counted_fh = CountingReader(raw_fh)
            context = ET.iterparse(counted_fh, events=("start", "end"))
            stack: list[str] = []

            for event, elem in context:
                tag = strip_ns(elem.tag).upper()

                if event == "start":
                    stack.append(tag)
                    if tag == "CONTENT":
                        content_started = True
                    continue

                in_content = "CONTENT" in stack and "SUBTEST" not in stack and "TESTDEF" not in stack

                if tag == "ITEM" and in_content:
                    item = " ".join(t.strip() for t in elem.itertext() if t and t.strip()).strip()
                    content_item_count += 1
                    parsed = parse_content_item(item)
                    rows.append({
                        "SourceFile": xml_file.name,
                        "SourcePath": str(xml_file),
                        "Cell": cell,
                        "Year": year,
                        "XMLModified": modified,
                        "FileSizeBytes": file_size,
                        "FileSizeMB": round(file_size / 1_000_000, 3),
                        "ContentIndex": content_item_count - 1,
                        **parsed,
                    })
                    elem.clear()

                elif tag == "CONTENT" and content_started:
                    content_done = True
                    elem.clear()
                    break

                else:
                    elem.clear()

                if stack:
                    stack.pop()

            bytes_read = counted_fh.bytes_read

        elapsed = time.perf_counter() - t0
        ls_nums = []
        m_nums = []
        has_ls = False
        has_m = False
        has_999 = False

        for r in rows:
            has_ls = has_ls or bool(r.get("IsLS"))
            has_m = has_m or bool(r.get("IsM"))
            has_999 = has_999 or bool(r.get("Is999"))
            if r.get("IsLS"):
                try:
                    ls_nums.append(int(r.get("SubtestNum") or 0))
                except Exception:
                    pass
            if r.get("IsM"):
                try:
                    m_nums.append(int(r.get("MNumber") or 0))
                except Exception:
                    pass

        summary = {
            "SourceFile": xml_file.name,
            "SourcePath": str(xml_file),
            "Cell": cell,
            "Year": year,
            "XMLModified": modified,
            "FileSizeBytes": file_size,
            "FileSizeMB": round(file_size / 1_000_000, 3),
            "BytesRead": bytes_read,
            "MBRead": round(bytes_read / 1_000_000, 3),
            "PercentFileScanned": round((bytes_read / file_size) * 100, 3) if file_size else "",
            "ContentFound": content_started,
            "ContentCompleted": content_done,
            "ContentItemCount": len(rows),
            "HasLS": has_ls,
            "HasM": has_m,
            "Has999": has_999,
            "MaxLSIndex": max(ls_nums) if ls_nums else "",
            "MaxMNumber": max(m_nums) if m_nums else "",
            "Seconds": round(elapsed, 4),
            "MBps": round((bytes_read / 1_000_000) / elapsed, 3) if elapsed > 0 else "",
            "Error": "",
        }
        return rows, summary, None

    except Exception as exc:
        elapsed = time.perf_counter() - t0
        summary = {
            "SourceFile": xml_file.name,
            "SourcePath": str(xml_file),
            "Cell": cell,
            "Year": year,
            "XMLModified": modified,
            "FileSizeBytes": file_size,
            "FileSizeMB": round(file_size / 1_000_000, 3),
            "BytesRead": bytes_read,
            "MBRead": round(bytes_read / 1_000_000, 3),
            "PercentFileScanned": round((bytes_read / file_size) * 100, 3) if file_size else "",
            "ContentFound": content_started,
            "ContentCompleted": content_done,
            "ContentItemCount": len(rows),
            "HasLS": "",
            "HasM": "",
            "Has999": "",
            "MaxLSIndex": "",
            "MaxMNumber": "",
            "Seconds": round(elapsed, 4),
            "MBps": "",
            "Error": repr(exc),
        }
        error = {"SourceFile": xml_file.name, "SourcePath": str(xml_file), "Error": repr(exc)}
        return rows, summary, error


def build_pattern_summary(inv: pd.DataFrame) -> pd.DataFrame:
    if inv.empty:
        return pd.DataFrame()
    group_cols = ["SubtestID", "SubtestType", "IsLS", "IsM", "Is999"]
    return (
        inv.groupby(group_cols, dropna=False)
           .agg(
               CountItems=("ContentItem", "size"),
               CountFiles=("SourceFile", "nunique"),
               ExampleItem=("ContentItem", "first"),
           )
           .reset_index()
           .sort_values(["CountItems", "CountFiles"], ascending=False)
    )


def build_unexpected(inv: pd.DataFrame) -> pd.DataFrame:
    if inv.empty or "ParseStatus" not in inv.columns:
        return pd.DataFrame()
    return inv[inv["ParseStatus"].ne("Parsed")].copy()


def write_workbook(out_path: Path, inventory_rows: list[dict[str, Any]], file_rows: list[dict[str, Any]], errors: list[dict[str, Any]]) -> None:
    inv = pd.DataFrame(inventory_rows)
    files = pd.DataFrame(file_rows)
    patterns = build_pattern_summary(inv)
    unexpected = build_unexpected(inv)
    errors_df = pd.DataFrame(errors)

    if not inv.empty:
        inv = inv.sort_values(["SourcePath", "ContentIndex"])
    if not files.empty:
        files = files.sort_values(["SourcePath"])

    summary = pd.DataFrame([
        {"Metric": "Files scanned", "Value": len(file_rows)},
        {"Metric": "Inventory rows", "Value": len(inv)},
        {"Metric": "Files with CONTENT", "Value": int(files["ContentFound"].fillna(False).sum()) if not files.empty and "ContentFound" in files.columns else 0},
        {"Metric": "Files with M subtests", "Value": int(files["HasM"].fillna(False).sum()) if not files.empty and "HasM" in files.columns else 0},
        {"Metric": "Files with 999 items", "Value": int(files["Has999"].fillna(False).sum()) if not files.empty and "Has999" in files.columns else 0},
        {"Metric": "Errors", "Value": len(errors)},
        {"Metric": "Total MB read", "Value": round(files["MBRead"].sum(), 3) if not files.empty and "MBRead" in files.columns else 0},
    ])

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        inv.to_excel(writer, sheet_name="Inventory", index=False)
        files.to_excel(writer, sheet_name="File_Summary", index=False)
        patterns.to_excel(writer, sheet_name="Pattern_Summary", index=False)
        unexpected.to_excel(writer, sheet_name="Unexpected_Patterns", index=False)
        summary.to_excel(writer, sheet_name="Run_Summary", index=False)
        if not errors_df.empty:
            errors_df.to_excel(writer, sheet_name="Errors", index=False)
        for ws in writer.book.worksheets:
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Lightweight CONTENT/ITEM subtest inventory scanner for Sakura XML logs.")
    ap.add_argument("inputs", nargs="+", help="XML file(s) or folder(s)")
    ap.add_argument("--recursive", action="store_true", help="Recursively scan folder inputs for XML files")
    ap.add_argument("--out", default="XMLLog_SubtestInventory.xlsx", help="Output .xlsx file")
    ap.add_argument("--max-files", type=int, default=None, help="Limit number of XML files for a short test run")
    ap.add_argument("--progress-every", type=int, default=100, help="Print progress every N files. Default: 100")
    args = ap.parse_args(argv)

    t0 = time.perf_counter()
    files = find_input_xmls(args.inputs, args.recursive, args.max_files)

    print()
    print("Sakura XML Subtest Inventory v01")
    print("=" * 72)
    print(f"XML files selected: {len(files):,}")
    print("Scan mode         : CONTENT/ITEM only")

    inventory_rows: list[dict[str, Any]] = []
    file_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for i, xml_file in enumerate(files, start=1):
        if i == 1 or (args.progress_every and i % args.progress_every == 0) or i == len(files):
            elapsed = time.perf_counter() - t0
            print(f"Scanning {i:,}/{len(files):,} | elapsed={elapsed:,.1f}s | inventory rows={len(inventory_rows):,} | file={xml_file.name}")

        rows, summary, error = inventory_file(xml_file)
        inventory_rows.extend(rows)
        file_rows.append(summary)
        if error:
            errors.append(error)

    out_path = Path(args.out)
    write_workbook(out_path, inventory_rows, file_rows, errors)

    elapsed = time.perf_counter() - t0
    total_mb = sum(float(r.get("MBRead", 0) or 0) for r in file_rows)

    print()
    print("Summary")
    print("-" * 72)
    print(f"XML files scanned : {len(file_rows):,}")
    print(f"Inventory rows    : {len(inventory_rows):,}")
    print(f"Errors            : {len(errors):,}")
    print(f"MB read           : {total_mb:,.3f}")
    print(f"Elapsed seconds   : {elapsed:,.3f}")
    print(f"Output            : {out_path}")
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
