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


# ============================================================
# User Configuration
# ============================================================

XML_DIR = Path(r"C:\Users\a357028\Desktop\Phase 1")

OUTPUT_DIR = XML_DIR / "merged_xml"
TEMP_DIR = XML_DIR / "_merge_temp"

# For local SSD, try 6-8.
# For network folders, start with 2-4.
MAX_WORKERS = 8

FILE_PATTERN = re.compile(
    r"HG02_(\d{4}-\d{2}-\d{2})_\d{4}\.xml$",
    re.IGNORECASE
)


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
# File Extraction Helper
# ============================================================

def extract_m1_data_to_temp(xml_file, channel_files, channel_written_flags):
    """
    Parse one XML file, find M1, append each M1 channel to temp files.

    Returns:
        samples, measure_end
    """
    tree = parse_xml(xml_file)
    root = tree.getroot()

    m1 = find_m1(root)

    if m1 is None:
        root.clear()
        del tree
        gc.collect()
        return 0, None

    mdef = m1.find("MDEF")

    samples = 0
    measure_end = None

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

    for par in m1.findall("PAR"):

        name = get_par_name(par)

        if not name:
            continue

        if name not in channel_files:
            continue

        value = get_par_value(par)

        channel_written_flags[name] = append_channel_value(
            channel_file=channel_files[name],
            value=value,
            already_written=channel_written_flags[name]
        )

    root.clear()
    del tree
    gc.collect()

    return samples, measure_end


# ============================================================
# Daily Worker Function
# ============================================================

def process_day(task):
    """
    Worker function for one day.
    This function runs in its own process.
    """
    day, day_file_strings, xml_dir_string, output_dir_string, temp_dir_string = task

    xml_dir = Path(xml_dir_string)
    output_dir = Path(output_dir_string)
    temp_dir = Path(temp_dir_string)

    day_files = [Path(f) for f in day_file_strings]

    output_file = output_dir / f"Merged_{day}.xml"
    day_temp_dir = temp_dir / day

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

        # ----------------------------------------------------
        # Load template file
        # ----------------------------------------------------

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

        channel_names = get_m1_channel_names(template_m1)

        if not channel_names:
            template_root.clear()
            del template_tree

            return {
                "day": day,
                "status": "failed",
                "message": "No M1 timeseries channels found",
                "samples": 0
            }

        print(f"[{day}] Channels: {len(channel_names)}")

        # ----------------------------------------------------
        # Set up temp channel files
        # ----------------------------------------------------

        channel_files = {
            name: day_temp_dir / safe_channel_filename(name)
            for name in channel_names
        }

        channel_written_flags = {
            name: False
            for name in channel_names
        }

        total_samples = 0
        final_measure_end = None

        # ----------------------------------------------------
        # Extract all files for this day
        # ----------------------------------------------------

        for idx, xml_file in enumerate(day_files, start=1):

            print(f"[{day}] [{idx}/{len(day_files)}] {xml_file.name}")

            samples, measure_end = extract_m1_data_to_temp(
                xml_file=xml_file,
                channel_files=channel_files,
                channel_written_flags=channel_written_flags
            )

            total_samples += samples

            if measure_end:
                final_measure_end = measure_end

        # ----------------------------------------------------
        # Update MDEF metadata
        # ----------------------------------------------------

        samples_par = get_mdef_parameter(template_mdef, "No_Samples")

        if samples_par is not None:
            set_par_value(samples_par, str(total_samples))

        end_par = get_mdef_parameter(template_mdef, "Tm_MeasureEnd")

        if end_par is not None and final_measure_end:
            set_par_value(end_par, final_measure_end)

        # ----------------------------------------------------
        # Replace M1 channel data from temp files
        # ----------------------------------------------------

        print(f"[{day}] Building final XML...")

        for par in template_m1.findall("PAR"):

            name = get_par_name(par)

            if not name:
                continue

            if name not in channel_files:
                continue

            merged_text = load_temp_channel_text(channel_files[name])
            set_par_value(par, merged_text)

        # ----------------------------------------------------
        # Write final daily XML
        # ----------------------------------------------------

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

        return {
            "day": day,
            "status": "complete",
            "message": f"Saved: {output_file}",
            "samples": total_samples
        }

    except Exception as e:

        error_text = traceback.format_exc()

        return {
            "day": day,
            "status": "error",
            "message": error_text,
            "samples": 0
        }


# ============================================================
# Main Program
# ============================================================

def main():

    OUTPUT_DIR.mkdir(exist_ok=True)
    TEMP_DIR.mkdir(exist_ok=True)

    # --------------------------------------------------------
    # Discover files
    # --------------------------------------------------------

    files_by_day = defaultdict(list)

    for xml_file in sorted(XML_DIR.glob("*.xml")):

        match = FILE_PATTERN.match(xml_file.name)

        if not match:
            continue

        day = match.group(1)
        files_by_day[day].append(xml_file)

    for day in files_by_day:
        files_by_day[day].sort()

    if not files_by_day:
        raise RuntimeError(f"No matching HG02 XML files found in: {XML_DIR}")

    total_files = sum(len(files) for files in files_by_day.values())
    total_days = len(files_by_day)

    print("============================================================")
    print("HG02 Daily XML Merge")
    print("============================================================")
    print(f"XML folder: {XML_DIR}")
    print(f"Output folder: {OUTPUT_DIR}")
    print(f"Temp folder: {TEMP_DIR}")
    print(f"Files found: {total_files}")
    print(f"Days found: {total_days}")

    workers = max(1, min(MAX_WORKERS, total_days, os.cpu_count() or 1))

    print(f"Worker processes: {workers}")
    print("============================================================")

    # --------------------------------------------------------
    # Build multiprocessing tasks
    # --------------------------------------------------------

    tasks = []

    for day, files in sorted(files_by_day.items()):

        task = (
            day,
            [str(f) for f in files],
            str(XML_DIR),
            str(OUTPUT_DIR),
            str(TEMP_DIR)
        )

        tasks.append(task)

    # --------------------------------------------------------
    # Run days in parallel
    # --------------------------------------------------------

    results = []

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

    # --------------------------------------------------------
    # Final summary
    # --------------------------------------------------------

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