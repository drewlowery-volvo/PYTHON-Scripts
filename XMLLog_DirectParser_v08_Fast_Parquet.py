#!/usr/bin/env python3
"""
XMLLog_DirectParser_v08_Parquet.py

Phase 2A multiprocessing parser for Sakura XML MasterLog extraction.
Merges v08 Fast optimizations with v06 Parquet & Partitioning capabilities:
- Parquet export (.parquet) by default via pyarrow with optional folder partitioning by Year
- Optional Excel export (.xlsx) with detailed summary and performance sheets
- Fast streaming parsing & C-accelerated lxml execution
- Remote HTTP range request streaming support
"""

from __future__ import annotations

import argparse
import io
import os
import re
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# Accelerate parsing using lxml C-engine if installed
try:
    import lxml.etree as ET
    USING_LXML = True
except ImportError:
    import xml.etree.ElementTree as ET
    USING_LXML = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import pandas as pd
except Exception as exc:
    raise SystemExit("ERROR: pandas is required. Install with: python -m pip install pandas openpyxl pyarrow") from exc

try:
    import openpyxl  # noqa: F401
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

try:
    import pyarrow  # noqa: F401
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

XML_NAME_RE = re.compile(r"^(HG\d{2})_(\d{4})-\d{2}-\d{2}_\d+", re.IGNORECASE)
TOKEN_RE = re.compile(r"^(\d+)([A-Za-z].*)$")

# Whitelist of fields to extract from XML nodes
WANTED_FIELDS = {
    "TestID", "SubtestIndex", "SubtestType", "M_ID",
    "RUN TYPE", "START TIME", "STOP TIME",
    "Eng_MapNo", "Operator", "Bad Data", "COMMENTS", "OperatorComment", "Eng_SerialNo", "Eng_Hours",
    "SCR_Catalyst", "DOC_SerialNo", "DPF_SerialNo", "SCR_SerialNo", "ECU",
    "Eng_EMS_Software", "Eng_EMS_Dataset", "Eng_ACM_Software", "Eng_ACM_Dataset",
    "TestReqOrder", "TestReqNo", "TestSpecifics", "TestName", "TestHeader", "TestDescription",
    "CycleStats", "Cycle1065",
    "Hum_InletAir", "P_Baro_Local", "CAC_EFF", "EGR_COOLER_EFF", "Energy_Cycle",
    "Spec_FCCycle", "Spec_UreaCycle", "BSUC", "BSTC", "BSCO2", "CARBON_BALANCE",
    "SOOT_RATIO", "SCR_BUFFER", "SULPHUR_RATIO", "CRYSTAL_RATIO",
    "Spec_NOxCycle", "Spec_NOCycle", "Spec_NO2Cycle",
    "Spec2_NOxCycle", "Spec2_NOCycle", "Spec2_NO2Cycle", "SPEC2_NOXCYCLE_BHPH",
    "Spec5_NOxCycle", "Spec5_NOCycle", "Spec5_NO2Cycle",
    "Spec_NOxCycleCVS", "Spec_CO2Cycle", "Spec5_CO2Cycle", "Spec2_CO2Cycle",
    "Spec_CO2CycleCVS", "Spec_N2OCycleCVS", "Spec2_N2OCycle",
    "Spec_CH4CycleCVS", "Spec2_CH4Cycle", "Spec_HCCycle", "Spec5_HCCycle", "Spec2_HCCycle", "Spec_HCCycleCVS",
    "Spec_COCycle", "Spec5_COCycle", "Spec2_COCycle", "Spec_COCycleCVS",
    "Spec_NOCycleCVS", "Spec_NO2CycleCVS",
    "Spec_PartCycle", "Spec_PartCycleCVS", "Ma_Partf_1_Befor", "Ma_Partf_1_After",
    "Spec_S483Cycle", "Spec_SootCCycle", "Conc2_NH3_Max", "Conc2_NH3_Avg",
    "FUEL_CONSUMPTION_GAL", "Mid_NO2_NOx_Ratio", "Mid_NO2_Nox_Ratio",
    "No_Samples", "Frq_Sampling", "Tm_LS", "Tm_MeasureStart",
    "Bin1_Windows", "Bin1_NOx", "Bin1_Soot", "Bin1_HC", "Bin1_CO",
    "Bin2_Windows", "Bin2_NOx", "Bin2_Soot", "Bin2_HC", "Bin2_CO",
    "Column10", "Spec_CbalFCCycleRaw"
}

# Updated explicitly requested column layout order
FRONT_COLUMNS = [
    "Year", "Cell", "SourceFile", "SourcePath", "XMLModified",
    "TestID", "SubTest Num", "RUN TYPE", "START TIME", "STOP TIME",
    "Eng_MapNo", "Operator", "Bad Data", "COMMENTS", "Eng_SerialNo",
    "Eng_Hours", "SCR_Catalyst", "DOC_SerialNo", "DPF_SerialNo", "SCR_SerialNo",
    "ECU", "Eng_EMS_Software", "Eng_EMS_Dataset", "Eng_ACM_Software",
    "Eng_ACM_Dataset", "TestReqOrder", "TestReqNo", "TestSpecifics",
    "TestName", "CycleStats", "Cycle1065", "Hum_InletAir", "P_Baro_Local",
    "CAC_EFF", "EGR_COOLER_EFF", "Energy_Cycle", "Spec_FCCycle", "BSUC",
    "BSTC", "BSCO2", "CARBON_BALANCE", "FUEL_CONSUMPTION_GAL", "SOOT_RATIO",
    "SCR_BUFFER", "SULPHUR_RATIO", "CRYSTAL_RATIO", "Spec_NOxCycle", "Spec5_NOxCycle",
    "Spec2_NOxCycle", "SPEC2_NOXCYCLE_BHPH", "Spec_NOxCycleCVS", "Ma_Partf_1_Befor",
    "Ma_Partf_1_After", "Spec_PartCycle", "Spec_S483Cycle", "Spec_SootCCycle",
    "Conc2_NH3_Max", "Conc2_NH3_Avg", "Spec_CO2Cycle", "Spec5_CO2Cycle",
    "Spec2_CO2Cycle", "Spec_CO2CycleCVS", "Spec2_N2OCycle", "Spec_N2OCycleCVS",
    "Spec2_CH4Cycle", "Spec_CH4CycleCVS", "Spec_NOCycle", "Spec5_NOCycle",
    "Spec2_NOCycle", "Spec_NOCycleCVS", "Spec_NO2Cycle", "Spec5_NO2Cycle",
    "Spec2_NO2Cycle", "Mid_NO2_Nox_Ratio", "Spec_NO2CycleCVS", "Spec_HCCycle",
    "Spec5_HCCycle", "Spec2_HCCycle", "Spec_HCCycleCVS", "Spec_COCycle",
    "Spec5_COCycle", "Spec2_COCycle", "Spec_COCycleCVS", "Bin1_Windows",
    "Bin1_NOx", "Bin1_Soot", "Bin1_HC", "Bin1_CO", "Bin2_Windows",
    "Bin2_NOx", "Bin2_Soot", "Bin2_HC", "Bin2_CO", "Column10",
    "Spec_UreaCycle", "Spec_CbalFCCycleRaw"
]

INDEX_COLUMNS = [
    "SourceFile", "TestID", "SubtestItem", "SubtestNum", "SubtestIndex", "SubtestID", "M_ID",
    "SubtestType", "RUN TYPE", "START TIME", "STOP TIME", "Eng_Hours", "TestReqNo", "TestName",
]


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


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def clean_text(text: Any) -> str:
    if text is None:
        return ""
    return str(text).strip()


def set_first_nonblank(row: dict[str, Any], name: str, value: Any) -> None:
    if not name or value in (None, ""):
        return
    if name not in row or row.get(name, "") in (None, ""):
        row[name] = value


def fast_par_scalar_local(
    par: Any, 
    include_all_parameters: bool, 
    semicolon_threshold: int, 
    counters: dict[str, int]
) -> tuple[str, str, bool]:
    counters["par_total"] += 1
    name = ""
    value = ""
    skipped_early = False

    for child in list(par):
        child_tag = strip_ns(child.tag).upper()
        if child_tag == "NAME":
            name = clean_text(child.text)
            if not include_all_parameters and name not in WANTED_FIELDS:
                counters["par_skipped"] += 1
                par.clear()
                return name, "", False
        elif child_tag == "VAL":
            if not name:
                continue
            
            raw_text = child.text
            if raw_text is None:
                continue

            if len(raw_text) > 100 and raw_text.count(";") > semicolon_threshold:
                counters["waveform_values"] += 1
                counters["par_skipped"] += 1
                par.clear()
                return name, "", False

            raw = clean_text(raw_text)
            if raw and raw.count(";") <= semicolon_threshold:
                value = raw
                break
            elif raw:
                counters["waveform_values"] += 1

    keep = bool(name and value and (include_all_parameters or name in WANTED_FIELDS))
    if keep:
        counters["par_kept"] += 1
    elif name and not skipped_early:
        counters["par_skipped"] += 1

    par.clear()
    return name, value, keep


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


def parse_item_token(item: str) -> tuple[str, str, str]:
    item = clean_text(item)
    if not item:
        return "", "", ""
    parts = item.split(maxsplit=1)
    token = parts[0]
    sub_type = parts[1] if len(parts) > 1 else ""
    m = TOKEN_RE.match(token)
    sub_id = m.group(2) if m else ""
    return token, sub_id, sub_type


def classify_run_type(subtest_type: str) -> str:
    s = str(subtest_type or "")
    mapping = [
        ("US_Trans_Cold", "Cold FTP"), ("US_Trans_Warm", "Hot FTP"),
        ("US_LLC_Warm", "LLC"), ("WHTC_Cold", "Cold WHTC"),
        ("WHTC_Warm", "Hot WHTC"), ("RMC_ESC", "RMC - ESC"),
        ("RMC", "RMC"), ("Low_NOx_Idle", "LNI"),
        ("FullLoad", "Max Power"), ("Full Load", "Max Power"),
        ("PowerSweep", "Power Sweep"), ("Power_Sweep", "Power Sweep"),
        ("TorqueCurve", "Torque Curve"), ("ScreeningTest", "Screening Test"),
        ("ResponseTest", "Response Test"), ("Special", "Special"),
        ("Calibration", "Calibration"), ("Soak", "Soak"),
    ]
    for key, value in mapping:
        if key.lower() in s.lower():
            return value
    return s


def to_float(x: Any) -> Optional[float]:
    try:
        if x in (None, ""):
            return None
        return float(x)
    except Exception:
        return None


def subtract(a: Any, b: Any) -> str | float:
    af = to_float(a)
    bf = to_float(b)
    if af is None or bf is None:
        return ""
    return af - bf


def ratio(a: Any, b: Any) -> str | float:
    af = to_float(a)
    bf = to_float(b)
    if af is None or bf in (None, 0):
        return ""
    return af / bf


def determine_catalyst(scr_serial: Any) -> str:
    scr = str(scr_serial or "").strip()
    if re.fullmatch(r"\d{3}-\d{4}", scr):
        return "Cu-Ze"
    if re.fullmatch(r"\d{14}", scr):
        return "Fe-Ze"
    return ""


def add_simple_derived(row: dict[str, Any]) -> None:
    """Calculates missing header values in the exact order specified."""
    # 1. Spec_NO2Cycle = Spec_NOxCycle - Spec_NOCycle
    if not row.get("Spec_NO2Cycle"):
        row["Spec_NO2Cycle"] = subtract(row.get("Spec_NOxCycle"), row.get("Spec_NOCycle"))

    # 2. Spec2_NO2Cycle = Spec2_NOxCycle - Spec2_NOCycle
    if not row.get("Spec2_NO2Cycle"):
        row["Spec2_NO2Cycle"] = subtract(row.get("Spec2_NOxCycle"), row.get("Spec2_NOCycle"))

    # 3. Spec5_NO2Cycle = Spec5_NOxCycle - Spec5_NOCycle
    if not row.get("Spec5_NO2Cycle"):
        row["Spec5_NO2Cycle"] = subtract(row.get("Spec5_NOxCycle"), row.get("Spec5_NOCycle"))

    # 4. Mid_NO2_Nox_Ratio = Spec5_NO2Cycle / Spec5_NOxCycle
    if not row.get("Mid_NO2_Nox_Ratio") and not row.get("Mid_NO2_NOx_Ratio"):
        r = ratio(row.get("Spec5_NO2Cycle"), row.get("Spec5_NOxCycle"))
        row["Mid_NO2_Nox_Ratio"] = r
        row["Mid_NO2_NOx_Ratio"] = r

    # 5. SCR_Catalyst determination from SCR_SerialNo pattern
    if not row.get("SCR_Catalyst"):
        row["SCR_Catalyst"] = determine_catalyst(row.get("SCR_SerialNo"))

    # 6. BSUC = Spec_UreaCycle
    if not row.get("BSUC"):
        row["BSUC"] = row.get("Spec_UreaCycle", "")

    # 7. BSTC = BSUC + Spec_FCCycle
    if not row.get("BSTC"):
        bsuc = to_float(row.get("BSUC"))
        bsfc = to_float(row.get("Spec_FCCycle"))
        row["BSTC"] = "" if bsuc is None or bsfc is None else bsuc + bsfc


def normalize_index(value: Any) -> str:
    txt = clean_text(value)
    if not txt:
        return ""
    try:
        f = float(txt)
        if f.is_integer():
            return str(int(f))
    except Exception:
        pass
    return txt


def resolve_content_item(content_by_token: dict[str, str], content_items: list[str], subtest_index: str, m_id: str, subtest_type: str) -> str:
    idx = normalize_index(subtest_index)
    mid = clean_text(m_id)
    stype = clean_text(subtest_type)
    if idx and mid:
        token = f"{idx}{mid}"
        if token in content_by_token:
            return content_by_token[token]
    try:
        ordinal = int(idx)
        if 0 <= ordinal < len(content_items):
            candidate = content_items[ordinal]
            if (not mid) or candidate.startswith(f"{idx}{mid}"):
                return candidate
    except Exception:
        pass
    if idx and mid and stype:
        return f"{idx}{mid} {stype}"
    if idx and stype:
        return f"{idx} {stype}"
    return "__MISSING_CONTENT_ITEM__"


def parse_subtest_filter_values(values: Optional[list[str]]) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    for value in values:
        for part in str(value).split(","):
            part = part.strip().lower()
            if part:
                out.append(part)
    return out


def should_keep_row(row: dict[str, Any], skip_calibration: bool, subtest_filters: list[str]) -> bool:
    sub_type = str(row.get("SubtestType", ""))
    run_type = str(row.get("RUN TYPE", ""))
    combined = f"{sub_type} {run_type}".lower()
    if skip_calibration and "calibration" in combined:
        return False
    if subtest_filters:
        return any(f in combined for f in subtest_filters)
    return True


def find_input_xmls(inputs: list[str], recursive: bool) -> list[Path]:
    files: list[Path] = []
    for item in inputs:
        if item.startswith(("http://", "https://")):
            files.append(Path(item))
            continue
        p = Path(item)
        if p.is_file() and p.suffix.lower() == ".xml":
            files.append(p)
        elif p.is_dir():
            files.extend(p.glob("**/*.xml" if recursive else "*.xml"))
        else:
            print(f"WARNING: input not found or not XML: {p}")
    return sorted(set(files), key=lambda x: str(x).lower())


def parse_xml_file_worker(args: tuple[Any, ...]) -> tuple[list[dict[str, Any]], dict[str, Any], Optional[dict[str, str]]]:
    (
        xml_path_str,
        include_all_parameters,
        semicolon_threshold,
        max_rows,
        early_stop,
        skip_calibration,
        subtest_filters,
        header_only_mode,
    ) = args
    
    xml_file = Path(xml_path_str)
    is_remote = xml_path_str.startswith(("http://", "https://"))
    counters = {"par_total": 0, "par_skipped": 0, "par_kept": 0, "waveform_values": 0, "rows_skipped_filter": 0}
    t0 = time.perf_counter()
    rows: list[dict[str, Any]] = []

    try:
        year = infer_year(xml_file)
        cell = infer_cell(xml_file)
        source_file = xml_file.name
        source_path = str(xml_file)
        modified = ""
        size_bytes = 0

        if not is_remote:
            try:
                modified = datetime.fromtimestamp(xml_file.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                size_bytes = xml_file.stat().st_size
            except Exception:
                pass

        content_items: list[str] = []
        content_by_token: dict[str, str] = {}
        global_pars: dict[str, Any] = {}
        current_subtest: Optional[dict[str, Any]] = None
        stack: list[str] = []
        bytes_read = 0
        subtests_seen = 0
        expected_content_items = 0
        early_stop_used = False

        if is_remote and header_only_mode:
            if not HAS_REQUESTS:
                raise ImportError("requests package is required for remote HTTP streaming.")
            headers = {"Range": "bytes=0-131072"}
            res = requests.get(xml_path_str, headers=headers)
            raw_bytes = res.content
            if b"</HEADER>" in raw_bytes.upper():
                raw_bytes = raw_bytes.split(b"</HEADER>")[0] + b"</HEADER></ROOT>"
            raw_fh = io.BytesIO(raw_bytes)
        elif is_remote:
            if not HAS_REQUESTS:
                raise ImportError("requests package is required for remote HTTP streaming.")
            res = requests.get(xml_path_str, stream=True)
            raw_fh = res.raw
        else:
            raw_fh = xml_file.open("rb", buffering=16 * 1024 * 1024)

        with raw_fh:
            counted_fh = CountingReader(raw_fh)
            context = ET.iterparse(counted_fh, events=("start", "end"))
            _, root = next(context)
            
            for event, elem in context:
                tag = strip_ns(elem.tag).upper()
                if event == "start":
                    stack.append(tag)
                    if tag == "SUBTEST":
                        current_subtest = {}
                    continue

                in_content = "CONTENT" in stack and "SUBTEST" not in stack and "TESTDEF" not in stack
                in_subtest = "SUBTEST" in stack
                in_testdef = "TESTDEF" in stack and not in_subtest

                if tag == "ITEM" and in_content:
                    item = " ".join(t.strip() for t in elem.itertext() if t and t.strip()).strip()
                    if item:
                        content_items.append(item)
                        expected_content_items = len(content_items)
                        token, _, _ = parse_item_token(item)
                        if token:
                            content_by_token[token] = item
                    elem.clear()

                elif tag == "PAR":
                    name, value, keep = fast_par_scalar_local(elem, include_all_parameters, semicolon_threshold, counters)
                    if keep:
                        if in_subtest and current_subtest is not None:
                            set_first_nonblank(current_subtest, name, value)
                        elif in_testdef:
                            set_first_nonblank(global_pars, name, value)

                elif tag == "SUBTEST":
                    subtests_seen += 1
                    local = current_subtest or {}
                    test_id = clean_text(global_pars.get("TestID")) or xml_file.stem
                    subtest_index = normalize_index(local.get("SubtestIndex", ""))
                    m_id = clean_text(local.get("M_ID", ""))
                    subtest_type_from_par = clean_text(local.get("SubtestType", ""))
                    item = resolve_content_item(content_by_token, content_items, subtest_index, m_id, subtest_type_from_par)
                    subtest_num, subtest_id_from_item, subtest_type_from_item = parse_item_token(item)
                    subtest_type = subtest_type_from_item or subtest_type_from_par
                    subtest_id = subtest_id_from_item or m_id
                    
                    row: dict[str, Any] = {
                        "Year": year, "Cell": cell, "SourceFile": source_file, "SourcePath": source_path,
                        "XMLModified": modified, "TestID": test_id, "SubtestItem": item,
                        "SubtestNum": subtest_num, "SubtestIndex": subtest_index, "SubtestID": subtest_id,
                        "M_ID": m_id, "SubtestType": subtest_type, "SubTest Num": subtest_num,
                    }
                    for k, v in global_pars.items():
                        set_first_nonblank(row, k, v)
                    for k, v in local.items():
                        set_first_nonblank(row, k, v)
                        
                    row.update({
                        "SubtestItem": item, "SubtestNum": subtest_num, 
                        "SubtestIndex": subtest_index, "SubtestID": subtest_id, 
                        "M_ID": m_id, "SubtestType": subtest_type, "SubTest Num": subtest_num
                    })

                    if not row.get("RUN TYPE"):
                        row["RUN TYPE"] = classify_run_type(subtest_type)
                    if not row.get("START TIME") and row.get("Tm_LS"):
                        row["START TIME"] = row.get("Tm_LS", "")
                    if not row.get("START TIME") and row.get("Tm_MeasureStart"):
                        row["START TIME"] = row.get("Tm_MeasureStart", "")

                    add_simple_derived(row)
                    
                    if should_keep_row(row, skip_calibration, subtest_filters):
                        rows.append(row)
                    else:
                        counters["rows_skipped_filter"] += 1
                        
                    current_subtest = None
                    elem.clear()

                    if max_rows is not None and len(rows) >= max_rows:
                        early_stop_used = True
                        break
                        
                    if early_stop and expected_content_items and subtests_seen >= expected_content_items:
                        early_stop_used = True
                        break

                elif tag in {"CONTENT", "TESTDEF", "SUBTESTDEF", "MDEF", "M", "PARAMS", "DESCRIPTIONS"}:
                    elem.clear()

                if stack:
                    stack.pop()
                    
            root.clear()
            bytes_read = counted_fh.bytes_read

        seconds = time.perf_counter() - t0
        mb_read = bytes_read / 1_000_000
        gb_read = bytes_read / 1_000_000_000
        stat = {
            "SourceFile": xml_file.name,
            "SourcePath": str(xml_file),
            "SizeBytes": size_bytes,
            "BytesRead": bytes_read,
            "MBRead": round(mb_read, 3),
            "RowsKept": len(rows),
            "RowsSkippedByFilter": counters["rows_skipped_filter"],
            "SubtestsSeen": subtests_seen,
            "ExpectedContentItems": expected_content_items,
            "EarlyStopUsed": early_stop_used,
            "ParseSeconds": round(seconds, 4),
            "MBps": round(mb_read / seconds, 3) if seconds > 0 else "",
            "RowsPerGB": round(len(rows) / gb_read, 3) if gb_read > 0 else "",
            "SecondsPerGB": round(seconds / gb_read, 3) if gb_read > 0 else "",
            "PAR_Total": counters["par_total"],
            "PAR_Skipped": counters["par_skipped"],
            "PAR_Kept": counters["par_kept"],
            "WaveformValues": counters["waveform_values"],
        }
        return rows, stat, None
    except Exception as exc:
        seconds = time.perf_counter() - t0
        stat = {"SourceFile": xml_file.name, "SourcePath": str(xml_file), "ParseSeconds": round(seconds, 4), "RowsKept": len(rows), "Error": repr(exc)}
        return [], stat, {"SourcePath": str(xml_file), "Error": repr(exc)}


def aggregate_perf(file_stats: list[dict[str, Any]], total_seconds: float) -> dict[str, Any]:
    total_bytes_read = sum(int(s.get("BytesRead", 0) or 0) for s in file_stats)
    total_mb_read = total_bytes_read / 1_000_000
    total_gb_read = total_bytes_read / 1_000_000_000
    par_total = sum(int(s.get("PAR_Total", 0) or 0) for s in file_stats)
    par_skipped = sum(int(s.get("PAR_Skipped", 0) or 0) for s in file_stats)
    par_kept = sum(int(s.get("PAR_Kept", 0) or 0) for s in file_stats)
    waveform = sum(int(s.get("WaveformValues", 0) or 0) for s in file_stats)
    rows_skipped = sum(int(s.get("RowsSkippedByFilter", 0) or 0) for s in file_stats)
    file_seconds_sum = sum(float(s.get("ParseSeconds", 0) or 0) for s in file_stats)
    return {
        "total_bytes_read": total_bytes_read,
        "total_mb_read": total_mb_read,
        "total_gb_read": total_gb_read,
        "mbps": total_mb_read / total_seconds if total_seconds > 0 else "",
        "par_total": par_total,
        "par_skipped": par_skipped,
        "par_kept": par_kept,
        "skip_ratio": round((par_skipped / par_total) * 100, 3) if par_total else "",
        "waveform_values": waveform,
        "rows_skipped_filter": rows_skipped,
        "file_seconds_sum": file_seconds_sum,
    }


def export_data(
    df: pd.DataFrame, 
    errors: list[dict[str, Any]], 
    file_stats: list[dict[str, Any]], 
    out_path: Path, 
    use_excel: bool, 
    partition_by_year: bool, 
    total_seconds: float, 
    workers: int
) -> None:
    if df.empty:
        raise RuntimeError("No rows were generated.")

    # Enforce exact column order while preserving any unlisted extras at the end
    first_cols = [c for c in FRONT_COLUMNS if c in df.columns]
    remaining = [c for c in df.columns if c not in first_cols]
    df = df[first_cols + remaining]

    sort_cols = [c for c in ["SourceFile", "SubtestIndex", "M_ID", "SubtestItem"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    if use_excel:
        if not HAS_OPENPYXL:
            raise ImportError("openpyxl is required to write Excel files.")
        unique_files = df["SourceFile"].nunique() if "SourceFile" in df.columns else ""
        missing_items = int((df.get("SubtestItem", pd.Series([], dtype=str)) == "__MISSING_CONTENT_ITEM__").sum()) if "SubtestItem" in df.columns else ""
        perf = aggregate_perf(file_stats, total_seconds)
        rows_per_sec = len(df) / total_seconds if total_seconds > 0 else ""
        rows_per_gb = len(df) / perf["total_gb_read"] if perf["total_gb_read"] > 0 else ""
        seconds_per_gb = total_seconds / perf["total_gb_read"] if perf["total_gb_read"] > 0 else ""
        
        summary = pd.DataFrame([
            {"Metric": "Rows written", "Value": len(df)},
            {"Metric": "Unique SourceFiles", "Value": unique_files},
            {"Metric": "Files processed", "Value": len(file_stats)},
            {"Metric": "Workers", "Value": workers},
            {"Metric": "Engine", "Value": "lxml (C-Engine)" if USING_LXML else "xml.etree (Python Standard)"},
            {"Metric": "Rows with missing content item", "Value": missing_items},
            {"Metric": "Errors", "Value": len(errors)},
            {"Metric": "Wall seconds", "Value": round(total_seconds, 3)},
            {"Metric": "Accumulated file parse seconds", "Value": round(perf["file_seconds_sum"], 3)},
            {"Metric": "Bytes read", "Value": perf["total_bytes_read"]},
            {"Metric": "MB read", "Value": round(perf["total_mb_read"], 3)},
            {"Metric": "GB read", "Value": round(perf["total_gb_read"], 3)},
            {"Metric": "Aggregate MB/sec", "Value": round(perf["mbps"], 3) if perf["mbps"] != "" else ""},
            {"Metric": "Rows/sec", "Value": round(rows_per_sec, 3) if rows_per_sec != "" else ""},
            {"Metric": "Rows/GB", "Value": round(rows_per_gb, 3) if perf["total_gb_read"] > 0 and rows_per_gb != "" else ""},
            {"Metric": "Seconds/GB", "Value": round(seconds_per_gb, 3) if perf["total_gb_read"] > 0 and seconds_per_gb != "" else ""},
            {"Metric": "PAR total", "Value": perf["par_total"]},
            {"Metric": "PAR skipped", "Value": perf["par_skipped"]},
            {"Metric": "PAR kept", "Value": perf["par_kept"]},
            {"Metric": "PAR skip ratio percent", "Value": perf["skip_ratio"]},
            {"Metric": "Waveform values skipped", "Value": perf["waveform_values"]},
            {"Metric": "Rows skipped by filter", "Value": perf["rows_skipped_filter"]},
        ])
        index_cols = [c for c in INDEX_COLUMNS if c in df.columns]
        subtest_index = df[index_cols].copy() if index_cols else pd.DataFrame()
        file_perf = pd.DataFrame(file_stats)

        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="MasterLog", index=False)
            subtest_index.to_excel(writer, sheet_name="Subtest_Index", index=False)
            summary.to_excel(writer, sheet_name="Summary", index=False)
            summary.to_excel(writer, sheet_name="Performance", index=False)
            if not file_perf.empty:
                file_perf.to_excel(writer, sheet_name="File_Performance", index=False)
            if errors:
                pd.DataFrame(errors).to_excel(writer, sheet_name="Errors", index=False)
            for ws in writer.book.worksheets:
                ws.freeze_panes = "A2"
                ws.auto_filter.ref = ws.dimensions
    else:
        # Format as parquet
        if not HAS_PYARROW:
            raise ImportError("pyarrow is required to write Parquet files.")
            
        # Cast all object/string columns explicitly to string to ensure clean Arrow schema
        df_parquet = df.copy()
        for col in df_parquet.columns:
            if df_parquet[col].dtype == "object":
                df_parquet[col] = df_parquet[col].astype(str)

        if partition_by_year and "Year" in df_parquet.columns:
            df_parquet.to_parquet(out_path, engine="pyarrow", compression="snappy", partition_cols=["Year"])
        else:
            df_parquet.to_parquet(out_path, engine="pyarrow", compression="snappy")


def run_sequential(files: list[Path], args, subtest_filters: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    stats: list[dict[str, Any]] = []
    run_t0 = time.perf_counter()
    for i, xml_file in enumerate(files, start=1):
        if args.max_rows is not None and len(rows) >= args.max_rows:
            print(f"Row limit reached before next file: {len(rows):,}/{args.max_rows:,}")
            break
        if i == 1 or (args.progress_every and i % args.progress_every == 0) or i == len(files):
            elapsed = time.perf_counter() - run_t0
            print(f"Processing {i}/{len(files)}: {xml_file} | elapsed={elapsed:,.1f}s | rows={len(rows):,}")
        remaining_rows = max(args.max_rows - len(rows), 0) if args.max_rows is not None else None
        task_args = (
            str(xml_file),
            args.include_all_parameters,
            args.semicolon_threshold,
            remaining_rows,
            not args.no_early_stop,
            args.skip_calibration,
            subtest_filters,
            args.header_only,
        )
        file_rows, stat, err = parse_xml_file_worker(task_args)
        rows.extend(file_rows)
        stats.append(stat)
        if err:
            errors.append(err)
        if args.max_rows is not None and len(rows) >= args.max_rows:
            rows = rows[:args.max_rows]
            print(f"Row limit reached: {len(rows):,}/{args.max_rows:,}")
            break
    return rows, errors, stats


def run_parallel(files: list[Path], args, subtest_filters: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    stats: list[dict[str, Any]] = []
    run_t0 = time.perf_counter()
    submitted = 0
    completed = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = []
        for xml_file in files:
            task_args = (
                str(xml_file),
                args.include_all_parameters,
                args.semicolon_threshold,
                None,
                not args.no_early_stop,
                args.skip_calibration,
                subtest_filters,
                args.header_only,
            )
            futures.append(ex.submit(parse_xml_file_worker, task_args))
            submitted += 1
        for fut in as_completed(futures):
            completed += 1
            file_rows, stat, err = fut.result()
            rows.extend(file_rows)
            stats.append(stat)
            if err:
                errors.append(err)
            if completed == 1 or (args.progress_every and completed % args.progress_every == 0) or completed == submitted:
                elapsed = time.perf_counter() - run_t0
                print(f"Completed {completed}/{submitted} files | elapsed={elapsed:,.1f}s | rows={len(rows):,}")
    if args.max_rows is not None and len(rows) > args.max_rows:
        rows = rows[:args.max_rows]
    return rows, errors, stats


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Phase 2A multiprocessing XML parser with Parquet & Excel export capabilities.")
    ap.add_argument("inputs", nargs="+", help="XML file(s), folder(s), or HTTP/S URLs")
    ap.add_argument("--recursive", action="store_true", help="Recursively scan folder inputs for XML files")
    ap.add_argument("--out", default="Master_XML_Log.parquet", help="Output .parquet file or destination folder")
    ap.add_argument("--excel", action="store_true", help="Export to Excel (.xlsx) instead of Parquet (.parquet)")
    ap.add_argument("--partition-by-year", action="store_true", help="Partition Parquet dataset into Year=YYYY subfolders")
    ap.add_argument("--include-all-parameters", action="store_true", help="Include all scalar PAR parameters, not just metadata whitelist")
    ap.add_argument("--semicolon-threshold", type=int, default=20, help="Skip values with more than this many semicolons. Default: 20")
    ap.add_argument("--max-rows", type=int, default=None, help="Stop/truncate after this many kept output rows.")
    ap.add_argument("--max-files", type=int, default=None, help="Stop after this many XML files")
    ap.add_argument("--progress-every", type=int, default=25, help="Print progress every N files. Default: 25")
    ap.add_argument("--no-early-stop", action="store_true", help="Disable early stop after expected CONTENT/ITEM subtests are seen")
    ap.add_argument("--header-only", action="store_true", help="Layer 1 Optimization: Fetch only top 128KB over HTTP range request")
    ap.add_argument("--skip-calibration", action="store_true", help="Exclude calibration subtests from output")
    ap.add_argument("--subtest-types", nargs="*", default=None, help="Only include subtest/run types matching these values.")
    ap.add_argument("--workers", type=int, default=4, help="Number of worker processes. Default: 4")
    args = ap.parse_args(argv)

    if not args.excel and not HAS_PYARROW:
        raise SystemExit("ERROR: pyarrow is required for Parquet export. Install with: python -m pip install pyarrow")

    if args.workers < 1:
        args.workers = 1

    files = find_input_xmls(args.inputs, args.recursive)
    total_files_found = len(files)
    if args.max_files is not None:
        files = files[:args.max_files]
    subtest_filters = parse_subtest_filter_values(args.subtest_types)

    print("\nDirect Sakura XML MasterLog Builder v08 (Parquet & Fast Engine)")
    print("=" * 72)
    print(f"XML Engine                       : {'lxml (C-Engine)' if USING_LXML else 'xml.etree (Python Standard)'}")
    print(f"Output Format                    : {'Excel (.xlsx)' if args.excel else 'Parquet (.parquet)'}")
    print(f"Partition by Year                : {args.partition_by_year}")
    print(f"XML files found                  : {total_files_found:,}")
    if args.max_files is not None:
        print(f"XML files selected by --max-files: {len(files):,}")
    print(f"Workers                          : {args.workers}")
    if args.max_rows is not None:
        print(f"Row limit enabled                : {args.max_rows:,}")
    print(f"Header-Only Mode                 : {args.header_only}")
    print(f"Early stop per XML               : {not args.no_early_stop}")
    print(f"Skip calibration                 : {args.skip_calibration}")
    print(f"Subtest filters                  : {subtest_filters if subtest_filters else 'none'}")

    t0 = time.perf_counter()
    if args.workers == 1:
        rows, errors, file_stats = run_sequential(files, args, subtest_filters)
    else:
        rows, errors, file_stats = run_parallel(files, args, subtest_filters)
    total_seconds = time.perf_counter() - t0

    if not rows:
        print("ERROR: No rows were generated.")
        return 1

    df = pd.DataFrame(rows)
    out_path = Path(args.out)
    
    export_data(
        df, errors, file_stats, out_path, 
        use_excel=args.excel, 
        partition_by_year=args.partition_by_year, 
        total_seconds=total_seconds, 
        workers=args.workers
    )
    
    perf = aggregate_perf(file_stats, total_seconds)
    rows_per_sec = len(df) / total_seconds if total_seconds > 0 else ""
    rows_per_gb = len(df) / perf["total_gb_read"] if perf["total_gb_read"] > 0 else ""
    seconds_per_gb = total_seconds / perf["total_gb_read"] if perf["total_gb_read"] > 0 else ""

    print("\nSummary")
    print("-" * 72)
    print(f"Engine                        : {'lxml (C-Engine)' if USING_LXML else 'xml.etree (Python Standard)'}")
    print(f"Output Format                 : {'Excel (.xlsx)' if args.excel else 'Parquet (.parquet)'}")
    print(f"Rows written                  : {len(df):,}")
    print(f"Unique SourceFiles            : {df['SourceFile'].nunique() if 'SourceFile' in df.columns else ''}")
    print(f"Workers                       : {args.workers}")
    print(f"Wall seconds                  : {round(total_seconds, 3)}")
    print(f"Accumulated file parse seconds: {round(perf['file_seconds_sum'], 3)}")
    print(f"MB read                       : {round(perf['total_mb_read'], 3)}")
    print(f"Aggregate MB/sec              : {round(perf['mbps'], 3) if perf['mbps'] != '' else ''}")
    print(f"Rows/sec                      : {round(rows_per_sec, 3) if rows_per_sec != '' else ''}")
    print(f"Rows/GB                       : {round(rows_per_gb, 3) if perf['total_gb_read'] > 0 and rows_per_gb != '' else ''}")
    print(f"Seconds/GB                    : {round(seconds_per_gb, 3) if perf['total_gb_read'] > 0 and seconds_per_gb != '' else ''}")
    print(f"PAR total                     : {perf['par_total']:,}")
    print(f"PAR skipped                   : {perf['par_skipped']:,}")
    print(f"PAR kept                      : {perf['par_kept']:,}")
    print(f"PAR skip ratio percent        : {perf['skip_ratio']}")
    print(f"Rows skipped by filter        : {perf['rows_skipped_filter']:,}")
    print(f"Errors                        : {len(errors):,}")
    print(f"Output saved to               : {out_path.resolve()}")

    return 0 if not errors else 1


if __name__ == "__main__":
    import multiprocessing as mp
    mp.freeze_support()
    raise SystemExit(main())