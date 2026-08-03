from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from lxml import etree as ET
import re
import shutil
import gc
import os
import hashlib
import traceback
import argparse

# ============================================================
# User Configuration
# ============================================================

SCRIPT_DIR = Path(__file__).resolve().parent

OUTPUT_DIR = SCRIPT_DIR / "Merged Daily XMLs"
TEMP_DIR = SCRIPT_DIR / "_merge_temp"

# For local SSD, try 6-8.
# For network folders, start with 2-4.
MAX_WORKERS = 8


# ============================================================
# XML Helper Functions
# ============================================================

def make_parser():
    return ET.XMLParser(
        huge_tree=True,
        recover=True,
        remove_blank_text=False
    )


def parse_xml(xml_file):
    return ET.parse(str(xml_file), make_parser())


def get_par_name(par):
    node = par.find("NAME")
    return node.text if node is not None else None


def get_par_value(par):
    node = par.find("VAL")
    return node.text if node is not None else None


def set_par_value(par, value):
    node = par.find("VAL")

    if node is None:
        node = ET.SubElement(par, "VAL")

    node.text = value


def find_m1(root):
    """
    Find the first M block where MDEF/PAR/NAME = M_ID and VAL = M1.
    """
    for m in root.findall(".//M"):

        mdef = m.find("MDEF")

        if mdef is None:
            continue

        for par in mdef.findall("PAR"):

            if get_par_name(par) == "M_ID" and get_par_value(par) == "M1":
                return m

    return None


def get_mdef_parameter(mdef, parameter_name):
    """
    Return PAR from MDEF by NAME.
    """
    if mdef is None:
        return None

    for par in mdef.findall("PAR"):

        if get_par_name(par) == parameter_name:
            return par

    return None


def get_m1_channel_names(m1):
    """
    Return channel names directly under M1.
    These are the timeseries PAR entries.
    """
    names = []

    for par in m1.findall("PAR"):

        name = get_par_name(par)

        if name:
            names.append(name)

    return names


def safe_channel_filename(channel_name):
    """
    Generate a safe unique filename for a channel.
    Hash avoids collisions if two names sanitize to the same text.
    """
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", channel_name)
    digest = hashlib.md5(channel_name.encode("utf-8")).hexdigest()[:8]

    return f"{clean}_{digest}.txt"


def append_channel_value(channel_file, value, already_written):
    """
    Append one semicolon-separated VAL block to a channel temp file.
    This avoids repeatedly building huge strings in memory.
    """
    if value is None:
        return already_written

    value = value.strip()

    if not value:
        return already_written

    value = value.strip(";")

    if not value:
        return already_written

    with open(channel_file, "a", encoding="utf-8", newline="") as f:

        if already_written:
            f.write(";")

        f.write(value)

    return True


def load_temp_channel_text(channel_file):
    if not channel_file.exists():
        return ""

    return channel_file.read_text(encoding="utf-8")



# ============================================================
# Sampling Frequency and Resampling Helpers
# ============================================================

REFERENCE_TIME_CHANNEL = "Tm_LS"


def split_semicolon_values(value_text):
    if value_text is None:
        return []
    value_text = value_text.strip().strip(";")
    if not value_text:
        return []
    return [item.strip() for item in value_text.split(";") if item.strip()]


def parse_time_token_to_seconds(token, reference_datetime=None):
    token = token.strip()
    try:
        return float(token), reference_datetime
    except ValueError:
        pass
    try:
        return float(token.replace(",", ".")), reference_datetime
    except ValueError:
        pass

    from datetime import datetime as _datetime
    candidate = token.replace("T", " ")
    datetime_formats = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S.%f",
        "%m/%d/%Y %H:%M:%S",
    ]
    parsed_dt = None
    for fmt in datetime_formats:
        try:
            parsed_dt = _datetime.strptime(candidate, fmt)
            break
        except ValueError:
            continue
    if parsed_dt is None:
        raise ValueError(f"Could not parse time token: {token}")
    if reference_datetime is None:
        reference_datetime = parsed_dt
    seconds = (parsed_dt - reference_datetime).total_seconds()
    return seconds, reference_datetime


def convert_time_tokens_to_seconds(time_tokens):
    seconds = []
    reference_datetime = None
    for token in time_tokens:
        sec, reference_datetime = parse_time_token_to_seconds(token, reference_datetime)
        seconds.append(sec)
    return seconds


def median_value(values):
    if not values:
        return None
    values = sorted(values)
    n = len(values)
    mid = n // 2
    if n % 2:
        return values[mid]
    return 0.5 * (values[mid - 1] + values[mid])


def estimate_frequency_from_time_tokens(time_tokens):
    if len(time_tokens) < 2:
        return None, None
    try:
        seconds = convert_time_tokens_to_seconds(time_tokens)
    except Exception:
        return None, None
    dts = []
    for i in range(1, len(seconds)):
        dt = seconds[i] - seconds[i - 1]
        if dt > 0:
            dts.append(dt)
    median_dt = median_value(dts)
    if median_dt is None or median_dt <= 0:
        return None, None
    return 1.0 / median_dt, median_dt


def build_time_based_keep_indices(time_tokens, target_hz):
    if target_hz is None:
        return None, None, None
    if target_hz <= 0:
        raise ValueError(f"Target frequency must be > 0 Hz. Got: {target_hz}")
    if not time_tokens:
        return [], None, None
    seconds = convert_time_tokens_to_seconds(time_tokens)
    detected_hz, median_dt = estimate_frequency_from_time_tokens(time_tokens)
    target_dt = 1.0 / float(target_hz)
    t0 = seconds[0]
    keep_indices = []
    last_bucket = None
    import math
    for idx, t in enumerate(seconds):
        elapsed = t - t0
        if elapsed < 0:
            continue
        bucket = int(math.floor((elapsed / target_dt) + 1e-9))
        if bucket != last_bucket:
            keep_indices.append(idx)
            last_bucket = bucket
    return keep_indices, detected_hz, median_dt


def apply_keep_indices_to_value_text(value_text, keep_indices):
    values = split_semicolon_values(value_text)
    if not values or not keep_indices:
        return ""
    max_index = len(values) - 1
    kept = []
    for idx in keep_indices:
        if idx <= max_index:
            kept.append(values[idx])
    return ";".join(kept)


def format_frequency_for_filename(freq_hz):
    if freq_hz is None:
        return ""
    text = f"{freq_hz:g}".replace(".", "p")
    return f"_{text}Hz"


# ============================================================
# All-Zero Channel Filtering Helpers
# ============================================================

REMOVE_ZERO_CHANNELS_BY_DEFAULT = True
ZERO_CHANNEL_TOLERANCE = 1e-12
ZERO_FILTER_PROTECTED_CHANNELS = {"Tm_LS"}


def is_all_zero_channel(value_text, zero_tolerance=ZERO_CHANNEL_TOLERANCE):
    # Return True only when VAL contains numeric zeros only.
    if value_text is None:
        return False

    value_text = value_text.strip().strip(";")

    if not value_text:
        return False

    saw_numeric_value = False

    for token in value_text.split(";"):
        token = token.strip()

        if not token:
            continue

        try:
            value = float(token)
        except ValueError:
            return False

        saw_numeric_value = True

        if abs(value) > zero_tolerance:
            return False

    return saw_numeric_value


def discover_active_channels_for_day(day_files, channel_names):
    # Determine which channels are non-zero at least once across the whole day.
    channel_set = set(channel_names)

    active_flags = {
        name: (name in ZERO_FILTER_PROTECTED_CHANNELS)
        for name in channel_names
    }

    for xml_file in day_files:
        tree = parse_xml(xml_file)
        root = tree.getroot()

        try:
            m1 = find_m1(root)

            if m1 is None:
                continue

            for par in m1.findall("PAR"):
                name = get_par_name(par)

                if not name:
                    continue

                if name not in channel_set:
                    continue

                if active_flags.get(name, False):
                    continue

                value = get_par_value(par)

                if not is_all_zero_channel(value):
                    active_flags[name] = True

        finally:
            root.clear()
            del tree
            gc.collect()

    active_channel_names = [
        name for name in channel_names
        if active_flags.get(name, False)
    ]

    removed_zero_channel_names = [
        name for name in channel_names
        if not active_flags.get(name, False)
    ]

    return active_channel_names, removed_zero_channel_names


def write_zero_filter_report(output_dir, day, test_cell, removed_zero_channel_names):
    # Write a traceability report listing channels removed for the day.
    if not removed_zero_channel_names:
        return None

    report_file = output_dir / f"RemovedZeroChannels_{test_cell}_{day}.txt"

    with open(report_file, "w", encoding="utf-8", newline="") as f:
        f.write(f"Removed all-zero channels for {test_cell} {day}\n")
        f.write("One channel per line. Constant non-zero channels are retained.\n")
        f.write("\n")

        for name in sorted(removed_zero_channel_names):
            f.write(name + "\n")

    return report_file

# ============================================================
# File Extraction Helper
# ============================================================

def extract_m1_data_to_temp(
    xml_file,
    channel_files,
    channel_written_flags,
    target_frequency_hz=None,
    reference_time_channel=REFERENCE_TIME_CHANNEL,
):
    tree = parse_xml(xml_file)
    root = tree.getroot()
    m1 = find_m1(root)
    if m1 is None:
        root.clear()
        del tree
        gc.collect()
        return 0, None, None, None, 0

    mdef = m1.find("MDEF")
    samples = 0
    measure_end = None
    detected_hz = None
    median_dt = None
    kept_samples = 0
    keep_indices = None

    samples_par = get_mdef_parameter(mdef, "No_Samples")
    if samples_par is not None:
        raw_samples = get_par_value(samples_par)
        if raw_samples:
            try:
                samples = int(float(raw_samples))
            except ValueError:
                samples = 0

    end_par = get_mdef_parameter(mdef, "Tm_MeasureEnd")
    if end_par is not None:
        measure_end = get_par_value(end_par)

    reference_value = None
    for par in m1.findall("PAR"):
        name = get_par_name(par)
        if name == reference_time_channel:
            reference_value = get_par_value(par)
            break

    reference_tokens = split_semicolon_values(reference_value)
    if reference_tokens:
        detected_hz, median_dt = estimate_frequency_from_time_tokens(reference_tokens)
        if target_frequency_hz is not None:
            keep_indices, detected_hz, median_dt = build_time_based_keep_indices(reference_tokens, target_frequency_hz)
            kept_samples = len(keep_indices)
        else:
            kept_samples = len(reference_tokens)
    elif target_frequency_hz is not None:
        raise RuntimeError(
            f"Reference time channel {reference_time_channel} not found or empty in {xml_file.name}. Cannot resample."
        )

    for par in m1.findall("PAR"):
        name = get_par_name(par)
        if not name:
            continue
        if name not in channel_files:
            continue
        value = get_par_value(par)
        if target_frequency_hz is not None:
            value = apply_keep_indices_to_value_text(value, keep_indices)
        channel_written_flags[name] = append_channel_value(
            channel_file=channel_files[name],
            value=value,
            already_written=channel_written_flags[name]
        )

    root.clear()
    del tree
    gc.collect()

    if target_frequency_hz is not None:
        return kept_samples, measure_end, detected_hz, median_dt, kept_samples
    return samples, measure_end, detected_hz, median_dt, kept_samples

# ============================================================
# Daily Worker Function
# ============================================================

def process_day(task):
    (
        day,
        test_cell,
        target_frequency_hz,
        write_zero_report_enabled,
        day_file_strings,
        xml_dir_string,
        output_dir_string,
        temp_dir_string,
    ) = task

    xml_dir = Path(xml_dir_string)
    output_dir = Path(output_dir_string)
    temp_dir = Path(temp_dir_string)
    day_files = [Path(f) for f in day_file_strings]

    frequency_suffix = format_frequency_for_filename(target_frequency_hz)
    output_file = output_dir / f"Merged_{test_cell}_{day}{frequency_suffix}.xml"
    day_temp_dir = temp_dir / f"{day}{frequency_suffix}"

    try:
        if output_file.exists():
            return {
                "day": day,
                "status": "skipped",
                "message": f"Output already exists: {output_file}",
                "samples": 0
            }

        if day_temp_dir.exists():
            shutil.rmtree(day_temp_dir)

        day_temp_dir.mkdir(parents=True, exist_ok=True)

        template_file = day_files[0]

        print(f"\n[{day}] Template: {template_file.name}")
        print(f"[{day}] Files: {len(day_files)}")

        if target_frequency_hz is not None:
            print(f"[{day}] Target output frequency: {target_frequency_hz:g} Hz")
        else:
            print(f"[{day}] Target output frequency: full resolution")

        template_tree = parse_xml(template_file)
        template_root = template_tree.getroot()

        template_m1 = find_m1(template_root)

        if template_m1 is None:
            template_root.clear()
            del template_tree
            return {
                "day": day,
                "status": "failed",
                "message": f"No M1 found in template file: {template_file.name}",
                "samples": 0
            }

        template_mdef = template_m1.find("MDEF")
        all_channel_names = get_m1_channel_names(template_m1)

        if not all_channel_names:
            template_root.clear()
            del template_tree
            return {
                "day": day,
                "status": "failed",
                "message": "No M1 timeseries channels found",
                "samples": 0
            }

        print(f"[{day}] Channels discovered: {len(all_channel_names)}")

        if REMOVE_ZERO_CHANNELS_BY_DEFAULT:
            active_channel_names, removed_zero_channel_names = discover_active_channels_for_day(
                day_files=day_files,
                channel_names=all_channel_names
            )
        else:
            active_channel_names = list(all_channel_names)
            removed_zero_channel_names = []

        active_channel_set = set(active_channel_names)
        removed_zero_channel_set = set(removed_zero_channel_names)

        print(f"[{day}] Channels kept: {len(active_channel_names)}")
        print(f"[{day}] All-zero channels removed: {len(removed_zero_channel_names)}")

        zero_report_file = None

        if write_zero_report_enabled:
            zero_report_file = write_zero_filter_report(
                output_dir=output_dir,
                day=day,
                test_cell=test_cell,
                removed_zero_channel_names=removed_zero_channel_names
            )

            if zero_report_file is not None:
                print(f"[{day}] Zero-channel report: {zero_report_file}")
        elif removed_zero_channel_names:
            print(f"[{day}] Zero-channel report disabled. Use --zero-report to write the .txt file.")

        channel_files = {
            name: day_temp_dir / safe_channel_filename(name)
            for name in active_channel_names
        }

        channel_written_flags = {
            name: False
            for name in active_channel_names
        }

        total_samples = 0
        final_measure_end = None
        detected_rates = []

        for idx, xml_file in enumerate(day_files, start=1):
            print(f"[{day}] [{idx}/{len(day_files)}] {xml_file.name}")

            samples, measure_end, detected_hz, median_dt, kept_samples = extract_m1_data_to_temp(
                xml_file=xml_file,
                channel_files=channel_files,
                channel_written_flags=channel_written_flags,
                target_frequency_hz=target_frequency_hz,
                reference_time_channel=REFERENCE_TIME_CHANNEL,
            )

            total_samples += samples

            if measure_end:
                final_measure_end = measure_end

            if detected_hz is not None:
                detected_rates.append(detected_hz)

            if detected_hz is not None and median_dt is not None:
                if target_frequency_hz is not None:
                    print(
                        f"[{day}]     detected {detected_hz:.3f} Hz "
                        f"(median dt {median_dt:.6f} s), kept {kept_samples} samples"
                    )
                else:
                    print(
                        f"[{day}]     detected {detected_hz:.3f} Hz "
                        f"(median dt {median_dt:.6f} s)"
                    )

        samples_par = get_mdef_parameter(template_mdef, "No_Samples")

        if samples_par is not None:
            set_par_value(samples_par, str(total_samples))

        sampling_par = get_mdef_parameter(template_mdef, "Sampling_Frequency")

        if sampling_par is not None and target_frequency_hz is not None:
            set_par_value(sampling_par, f"{target_frequency_hz:g}")

        end_par = get_mdef_parameter(template_mdef, "Tm_MeasureEnd")

        if end_par is not None and final_measure_end:
            set_par_value(end_par, final_measure_end)

        print(f"[{day}] Building final XML...")

        removed_from_template = 0

        for par in list(template_m1.findall("PAR")):
            name = get_par_name(par)

            if not name:
                continue

            if name in removed_zero_channel_set:
                template_m1.remove(par)
                removed_from_template += 1
                continue

            if name not in active_channel_set:
                continue

            merged_text = load_temp_channel_text(channel_files[name])

            if not merged_text and name not in ZERO_FILTER_PROTECTED_CHANNELS:
                template_m1.remove(par)
                removed_from_template += 1
                continue

            set_par_value(par, merged_text)

        print(f"[{day}] Removed from template: {removed_from_template}")


        # ----------------------------------------------------
        # Swap ECU### channel NAME/DESC before writing final XML
        # ----------------------------------------------------

        ecu_swap_count = swap_ecu_name_desc_in_m1(template_m1)
        print(f"[{day}] ECU NAME/DESC swaps: {ecu_swap_count}")

        template_tree.write(
            str(output_file),
            encoding="ISO-8859-1",
            xml_declaration=True,
            pretty_print=False
        )

        template_root.clear()
        del template_tree

        shutil.rmtree(day_temp_dir)
        gc.collect()

        avg_rate_text = "not detected"

        if detected_rates:
            avg_rate = sum(detected_rates) / len(detected_rates)
            avg_rate_text = f"{avg_rate:.3f} Hz"

        return {
            "day": day,
            "status": "complete",
            "message": (
                f"Saved: {output_file}\n"
                f"Detected input frequency: {avg_rate_text}\n"
                f"Channels discovered: {len(all_channel_names)}\n"
                f"Channels kept: {len(active_channel_names)}\n"
                f"All-zero channels removed: {len(removed_zero_channel_names)}"
            ),
            "samples": total_samples
        }

    except Exception:
        error_text = traceback.format_exc()

        try:
            if day_temp_dir.exists():
                shutil.rmtree(day_temp_dir)
        except Exception:
            pass

        return {
            "day": day,
            "status": "error",
            "message": error_text,
            "samples": 0
        }


# ============================================================
# ECU Channel Name Cleanup Helpers
# ============================================================

ECU_NAME_DESC_SWAP_ENABLED = True


def swap_ecu_name_desc_in_m1(m1):
    # Permanently swap ECU### NAME fields with their DESC text.
    # Example:
    #   NAME=ECU045, DESC=fan_DutyCycle
    # becomes:
    #   NAME=fan_DutyCycle, DESC=ECU045
    # This is applied after filtering/merging and before writing final XML.
    if not ECU_NAME_DESC_SWAP_ENABLED:
        return 0

    swap_count = 0

    for par in m1.findall('PAR'):
        name_node = par.find('NAME')
        desc_node = par.find('DESC')

        if name_node is None or desc_node is None:
            continue

        if name_node.text is None or desc_node.text is None:
            continue

        old_name = name_node.text.strip()
        old_desc = desc_node.text.strip()

        if not old_name or not old_desc:
            continue

        if old_name.startswith('ECU'):
            name_node.text = old_desc
            desc_node.text = old_name
            swap_count += 1

    return swap_count

# ============================================================
# Main Program
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Daily XML Merge"
    )

    parser.add_argument(
        "xml_dir",
        help="Directory containing XML files"
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help="Number of worker processes"
    )

    parser.add_argument(
        "--freq",
        type=float,
        default=None,
        help="Optional target output sampling frequency in Hz. If omitted, full resolution is preserved."
    )

    parser.add_argument(
        "--zero-report",
        action="store_true",
        help="Write RemovedZeroChannels_*.txt reports. Disabled by default."
    )



    parser.add_argument(
        "--start-file",
        default=None,
        help="Optional first XML filename to include, inclusive. Example: HG26_2025-03-11_1327.xml"
    )

    parser.add_argument(
        "--end-file",
        default=None,
        help="Optional last XML filename to include, inclusive. Example: HG26_2025-08-27_1348.xml"
    )

    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional first date to include, inclusive, in YYYY-MM-DD format."
    )

    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional last date to include, inclusive, in YYYY-MM-DD format."
    )

    args = parser.parse_args()

    xml_dir = Path(args.xml_dir)

    if not xml_dir.exists():
        raise RuntimeError(
            f"Input directory does not exist: {xml_dir}"
        )

    # Detect test cell from path, such as HG26, HG27, HG31, etc.
    test_cell = None

    for part in xml_dir.parts:
        part_upper = part.upper()

        if re.fullmatch(r"HG\d+", part_upper):
            test_cell = part_upper
            break

    if test_cell is None:
        raise RuntimeError(
            f"Could not determine test cell from path: {xml_dir}"
        )

    print(f"Detected test cell: {test_cell}")

    # Build file pattern dynamically from detected test cell.
    # Example: HG26_2025-03-11_0000.xml
    file_pattern = re.compile(
        rf"{re.escape(test_cell)}_(\d{{4}}-\d{{2}}-\d{{2}})_\d{{4}}\.xml$",
        re.IGNORECASE
    )

    OUTPUT_DIR.mkdir(exist_ok=True)
    TEMP_DIR.mkdir(exist_ok=True)

    files_by_day = defaultdict(list)
    matching_files_total = 0
    selected_files_total = 0

    start_file_name = Path(args.start_file).name if args.start_file else None
    end_file_name = Path(args.end_file).name if args.end_file else None
    start_date = args.start_date
    end_date = args.end_date

    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    if start_date is not None and not date_pattern.match(start_date):
        raise RuntimeError(
            f"Invalid --start-date format: {start_date}. Expected YYYY-MM-DD."
        )

    if end_date is not None and not date_pattern.match(end_date):
        raise RuntimeError(
            f"Invalid --end-date format: {end_date}. Expected YYYY-MM-DD."
        )

    if start_date is not None and end_date is not None and start_date > end_date:
        raise RuntimeError(
            f"Invalid date range: --start-date {start_date} is after --end-date {end_date}."
        )

    if start_file_name is not None and end_file_name is not None and start_file_name > end_file_name:
        raise RuntimeError(
            f"Invalid file range: --start-file {start_file_name} is after --end-file {end_file_name}."
        )

    for xml_file in sorted(xml_dir.glob("*.xml")):

        match = file_pattern.match(xml_file.name)

        if not match:
            continue

        matching_files_total += 1

        day = match.group(1)
        file_name = xml_file.name

        if start_file_name is not None and file_name < start_file_name:
            continue

        if end_file_name is not None and file_name > end_file_name:
            continue

        if start_date is not None and day < start_date:
            continue

        if end_date is not None and day > end_date:
            continue

        selected_files_total += 1
        files_by_day[day].append(xml_file)

    for day in files_by_day:
        files_by_day[day].sort()

    if not files_by_day:
        raise RuntimeError(
            f"No matching XML files found for {test_cell} in: {xml_dir}"
        )

    total_files = sum(len(files) for files in files_by_day.values())
    total_days = len(files_by_day)

    print("============================================================")
    print("Daily XML Merge")
    print("============================================================")
    print(f"XML folder: {xml_dir}")
    print(f"Start file: {start_file_name if start_file_name else '(none)'}")
    print(f"End file: {end_file_name if end_file_name else '(none)'}")
    print(f"Start date: {start_date if start_date else '(none)'}")
    print(f"End date: {end_date if end_date else '(none)'}")
    print(f"Matching files before range filter: {matching_files_total}")
    print(f"Selected files after range filter: {selected_files_total}")
    print(f"Detected test cell: {test_cell}")
    print(f"Output folder: {OUTPUT_DIR}")
    print(f"Temp folder: {TEMP_DIR}")
    print(f"Files found: {total_files}")
    print(f"Days found: {total_days}")

    workers = max(1, min(args.workers, total_days, os.cpu_count() or 1))

    print(f"Worker processes: {workers}")
    print("============================================================")

    tasks = []

    for day, files in sorted(files_by_day.items()):

        task = (
            day,
            test_cell,
            args.freq,
            args.zero_report,
            [str(f) for f in files],
            str(xml_dir),
            str(OUTPUT_DIR),
            str(TEMP_DIR)
        )

        tasks.append(task)

    results = []

    try:

        with ProcessPoolExecutor(max_workers=workers) as executor:

            futures = {
                executor.submit(process_day, task): task[0]
                for task in tasks
            }

            for future in as_completed(futures):

                day = futures[future]

                try:
                    result = future.result()
                    results.append(result)

                    print("\n------------------------------------------------------------")
                    print(f"Day: {result['day']}")
                    print(f"Status: {result['status']}")
                    print(f"Samples: {result['samples']}")
                    print(result["message"])
                    print("------------------------------------------------------------")

                except Exception as e:

                    print("\n------------------------------------------------------------")
                    print(f"Day: {day}")
                    print("Status: error")
                    print(str(e))
                    print("------------------------------------------------------------")

    except KeyboardInterrupt:

        print("\n")
        print("============================================================")
        print("Keyboard interrupt received (Ctrl+C)")
        print("Stopping worker processes...")
        print("Completed daily XML files will be preserved.")
        print("Incomplete days may need to be re-run.")
        print("============================================================")

        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

        raise SystemExit(1)

    completed = sum(1 for r in results if r["status"] == "complete")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed = sum(1 for r in results if r["status"] in {"failed", "error"})

    print("\n============================================================")
    print("Finished")
    print("============================================================")
    print(f"Completed days: {completed}")
    print(f"Skipped days:   {skipped}")
    print(f"Failed days:    {failed}")
    print(f"Output folder:  {OUTPUT_DIR}")
    print("============================================================")

# Required for multiprocessing on Windows
if __name__ == "__main__":
    main()