from pathlib import Path
from lxml import etree as ET
import re
import shutil
import gc
import hashlib
import traceback


# ============================================================
# User Configuration
# ============================================================

BASE_DIR = Path(r"C:\Users\a357028\Desktop\Phase 1")

DAILY_XML_DIR = BASE_DIR / "merged_xml"
TEMP_DIR = BASE_DIR / "_master_merge_temp_1hz"

# Output is intentionally different from the full-resolution master file.
OUTPUT_FILE = BASE_DIR / "Master_Merged_M1_1Hz.xml"

# 10 Hz -> 1 Hz means keep 1 out of every 10 samples.
DOWNSAMPLE_FACTOR = 10
NEW_SAMPLING_FREQUENCY_HZ = 1

DAILY_FILE_PATTERN = re.compile(
    r"Merged_(\d{4}-\d{2}-\d{2})\.xml$",
    re.IGNORECASE
)

# Prefer Tm_LS as the reference channel for new No_Samples and final end time.
REFERENCE_TIME_CHANNEL = "Tm_LS"


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
    """
    Parse XML using an explicit binary file handle.

    This avoids lxml path/URL issues on Windows, especially with spaces
    in folder names such as "Phase 1".
    """
    with open(xml_file, "rb") as f:
        return ET.parse(f, make_parser())


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
    Find the M block where MDEF/PAR/NAME = M_ID and VAL = M1.
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
    Return all timeseries channel names directly under M1.
    """
    names = []

    for par in m1.findall("PAR"):

        name = get_par_name(par)

        if name:
            names.append(name)

    return names


def safe_channel_filename(channel_name):
    """
    Create a safe unique temp filename for each channel.
    """
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", channel_name)
    digest = hashlib.md5(channel_name.encode("utf-8")).hexdigest()[:8]

    return f"{clean}_{digest}.txt"


def append_downsampled_channel_value(
    channel_file,
    value,
    already_written,
    sample_counter,
    downsample_factor,
):
    """
    Append downsampled semicolon-separated channel data to disk.

    Important:
    - Uses a running per-channel sample_counter so the 1 Hz cadence is
      continuous across daily XML file boundaries.
    - For 10 Hz -> 1 Hz, keeps global sample indices 0, 10, 20, 30, ...

    Returns:
        already_written, new_sample_counter, kept_count, last_kept_value
    """
    if value is None:
        return already_written, sample_counter, 0, None

    value = value.strip().strip(";")

    if not value:
        return already_written, sample_counter, 0, None

    kept_values = []
    last_kept_value = None

    # Split this VAL block only once. This is much faster than repeated string
    # concatenation and lets us downsample before writing to disk.
    for item in value.split(";"):
        item = item.strip()

        if item:
            if sample_counter % downsample_factor == 0:
                kept_values.append(item)
                last_kept_value = item

            sample_counter += 1

    if not kept_values:
        return already_written, sample_counter, 0, None

    with open(channel_file, "a", encoding="utf-8", newline="") as f:

        if already_written:
            f.write(";")

        f.write(";".join(kept_values))

    return True, sample_counter, len(kept_values), last_kept_value


def load_temp_channel_text(channel_file):
    if not channel_file.exists():
        return ""

    return channel_file.read_text(encoding="utf-8")


def normalize_time_for_mdef(time_text):
    """
    MDEF Tm_MeasureEnd usually appears as 'YYYY-MM-DD HH:MM:SS'.
    Tm_LS channel values may include milliseconds. This trims milliseconds
    for metadata while preserving channel data as-is.
    """
    if not time_text:
        return time_text

    time_text = str(time_text).strip()

    if len(time_text) >= 19:
        return time_text[:19]

    return time_text


def validate_daily_files(daily_files):
    """
    Validate that daily XML files exist and are not zero bytes.

    This catches missing, incomplete, or failed daily outputs before the
    master merge spends time processing other files.
    """
    print("\nChecking daily XML files...")

    valid_daily_files = []

    for f in daily_files:
        if not f.exists():
            print(f"Missing file, skipping: {f}")
            continue

        try:
            size = f.stat().st_size
        except OSError as e:
            print(f"Cannot read file metadata, skipping: {f} -- {e}")
            continue

        if size == 0:
            print(f"Zero-byte file, skipping: {f}")
            continue

        print(f"OK: {f.name}  ({size / 1024 / 1024:.1f} MB)")
        valid_daily_files.append(f)

    if not valid_daily_files:
        raise RuntimeError("No valid daily XML files remain after validation.")

    return valid_daily_files


# ============================================================
# Extract and Downsample M1 From One Daily XML
# ============================================================

def extract_m1_data_to_temp_1hz(
    xml_file,
    channel_files,
    channel_written_flags,
    channel_sample_counters,
):
    """
    Parse one daily XML file, downsample each M1 timeseries channel from
    10 Hz to 1 Hz, and append it to temp text files.

    Returns:
        kept_reference_samples, measure_start, measure_end, last_kept_reference_time
    """

    tree = parse_xml(xml_file)
    root = tree.getroot()

    m1 = find_m1(root)

    if m1 is None:
        root.clear()
        del tree
        gc.collect()
        return 0, None, None, None

    mdef = m1.find("MDEF")

    measure_start = None
    measure_end = None
    kept_reference_samples = 0
    last_kept_reference_time = None

    start_par = get_mdef_parameter(mdef, "Tm_MeasureStart")

    if start_par is not None:
        measure_start = get_par_value(start_par)

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

        (
            channel_written_flags[name],
            channel_sample_counters[name],
            kept_count,
            last_kept_value,
        ) = append_downsampled_channel_value(
            channel_file=channel_files[name],
            value=value,
            already_written=channel_written_flags[name],
            sample_counter=channel_sample_counters[name],
            downsample_factor=DOWNSAMPLE_FACTOR,
        )

        if name == REFERENCE_TIME_CHANNEL:
            kept_reference_samples += kept_count
            if last_kept_value:
                last_kept_reference_time = last_kept_value

    root.clear()
    del tree
    gc.collect()

    return kept_reference_samples, measure_start, measure_end, last_kept_reference_time


# ============================================================
# Main Program
# ============================================================

def main():

    print("============================================================")
    print("HG02 Master XML Merge with 10 Hz -> 1 Hz Downsampling")
    print("============================================================")
    print(f"Daily XML folder:       {DAILY_XML_DIR}")
    print(f"Output file:            {OUTPUT_FILE}")
    print(f"Temp folder:            {TEMP_DIR}")
    print(f"Downsample factor:      {DOWNSAMPLE_FACTOR}")
    print(f"New sampling frequency: {NEW_SAMPLING_FREQUENCY_HZ} Hz")
    print("============================================================")

    if not DAILY_XML_DIR.exists():
        raise RuntimeError(f"Daily XML folder does not exist: {DAILY_XML_DIR}")

    # --------------------------------------------------------
    # Find daily XML files
    # --------------------------------------------------------

    daily_files = []

    for xml_file in DAILY_XML_DIR.glob("*.xml"):

        match = DAILY_FILE_PATTERN.match(xml_file.name)

        if match:
            daily_files.append(xml_file)

    daily_files.sort()

    if not daily_files:
        raise RuntimeError(f"No daily merged XML files found in: {DAILY_XML_DIR}")

    print(f"Daily XML files found before validation: {len(daily_files)}")

    # Validate files before parsing. This catches missing/zero-byte files.
    daily_files = validate_daily_files(daily_files)

    print(f"Daily XML files after validation: {len(daily_files)}")

    if OUTPUT_FILE.exists():
        raise RuntimeError(
            f"Output file already exists:\n{OUTPUT_FILE}\n\n"
            "Delete it or rename it before running this script."
        )

    # --------------------------------------------------------
    # Clean temp folder
    # --------------------------------------------------------

    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR)

    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # ----------------------------------------------------
        # Use first valid daily file as master template
        # ----------------------------------------------------

        template_file = daily_files[0]

        print(f"\nTemplate file: {template_file.name}")

        template_tree = parse_xml(template_file)
        template_root = template_tree.getroot()

        template_m1 = find_m1(template_root)

        if template_m1 is None:
            raise RuntimeError(f"No M1 found in template file: {template_file}")

        template_mdef = template_m1.find("MDEF")

        channel_names = get_m1_channel_names(template_m1)

        if not channel_names:
            raise RuntimeError("No M1 timeseries channels found in template file.")

        print(f"M1 channels found: {len(channel_names)}")

        if REFERENCE_TIME_CHANNEL not in channel_names:
            print(
                f"WARNING: Reference time channel '{REFERENCE_TIME_CHANNEL}' was not found. "
                "No_Samples will be estimated from the first channel instead."
            )

        # ----------------------------------------------------
        # Create temp channel files and downsample state
        # ----------------------------------------------------

        channel_files = {
            name: TEMP_DIR / safe_channel_filename(name)
            for name in channel_names
        }

        channel_written_flags = {
            name: False
            for name in channel_names
        }

        # This preserves 1 Hz phase continuously across daily file boundaries.
        channel_sample_counters = {
            name: 0
            for name in channel_names
        }

        total_1hz_samples = 0
        master_measure_start = None
        master_measure_end = None
        last_kept_reference_time = None

        # ----------------------------------------------------
        # Append all downsampled daily M1 data to temp files
        # ----------------------------------------------------

        for idx, xml_file in enumerate(daily_files, start=1):

            print(f"[{idx}/{len(daily_files)}] Reading and downsampling {xml_file.name}")

            (
                kept_reference_samples,
                measure_start,
                measure_end,
                last_ref_time_this_file,
            ) = extract_m1_data_to_temp_1hz(
                xml_file=xml_file,
                channel_files=channel_files,
                channel_written_flags=channel_written_flags,
                channel_sample_counters=channel_sample_counters,
            )

            total_1hz_samples += kept_reference_samples

            if master_measure_start is None and measure_start:
                master_measure_start = measure_start

            # Fallback to original daily MDEF end time if reference channel is missing.
            if measure_end:
                master_measure_end = measure_end

            if last_ref_time_this_file:
                last_kept_reference_time = last_ref_time_this_file

        # If Tm_LS was missing, estimate output samples from the first available channel.
        if total_1hz_samples == 0 and channel_names:
            first_channel = channel_names[0]
            first_channel_file = channel_files[first_channel]
            if first_channel_file.exists() and first_channel_file.stat().st_size > 0:
                text = first_channel_file.read_text(encoding="utf-8").strip().strip(";")
                total_1hz_samples = 0 if not text else text.count(";") + 1

        # Prefer the last kept 1 Hz timestamp for MDEF end time.
        if last_kept_reference_time:
            master_measure_end = normalize_time_for_mdef(last_kept_reference_time)

        print("\nFinished reading and downsampling daily XML files.")
        print(f"Total 1 Hz samples: {total_1hz_samples}")

        # ----------------------------------------------------
        # Update MDEF metadata in master template
        # ----------------------------------------------------

        samples_par = get_mdef_parameter(template_mdef, "No_Samples")

        if samples_par is not None:
            set_par_value(samples_par, str(total_1hz_samples))

        sampling_par = get_mdef_parameter(template_mdef, "Frq_Sampling")

        if sampling_par is not None:
            set_par_value(sampling_par, str(NEW_SAMPLING_FREQUENCY_HZ))

        start_par = get_mdef_parameter(template_mdef, "Tm_MeasureStart")

        if start_par is not None and master_measure_start:
            set_par_value(start_par, master_measure_start)

        end_par = get_mdef_parameter(template_mdef, "Tm_MeasureEnd")

        if end_par is not None and master_measure_end:
            set_par_value(end_par, master_measure_end)

        # ----------------------------------------------------
        # Replace template M1 channel values with downsampled data
        # ----------------------------------------------------

        print("\nBuilding 1 Hz master XML...")

        for par in template_m1.findall("PAR"):

            name = get_par_name(par)

            if not name:
                continue

            if name not in channel_files:
                continue

            merged_text = load_temp_channel_text(channel_files[name])
            set_par_value(par, merged_text)

        # ----------------------------------------------------
        # Write 1 Hz master XML
        # ----------------------------------------------------

        template_tree.write(
            str(OUTPUT_FILE),
            encoding="ISO-8859-1",
            xml_declaration=True,
            pretty_print=False
        )

        print("\n============================================================")
        print("1 Hz master merge complete.")
        print("============================================================")
        print(f"Output file:\n{OUTPUT_FILE}")
        print(f"Total 1 Hz samples: {total_1hz_samples}")
        print("============================================================")

        template_root.clear()
        del template_tree
        gc.collect()

    except Exception:

        print("\nERROR OCCURRED:")
        print(traceback.format_exc())
        raise

    finally:

        # ----------------------------------------------------
        # Clean up temp files
        # ----------------------------------------------------

        if TEMP_DIR.exists():
            shutil.rmtree(TEMP_DIR)

        gc.collect()


if __name__ == "__main__":
    main()
