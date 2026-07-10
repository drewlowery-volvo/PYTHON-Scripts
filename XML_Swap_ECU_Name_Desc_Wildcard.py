import sys
import glob
from pathlib import Path
from lxml import etree as ET

parser = ET.XMLParser(huge_tree=True, recover=True)

if len(sys.argv) < 2:
    print(
        "\nUsage:"
        "\n  python XML_Swap_ECU_Name_Desc_Wildcard_v2.py file.xml"
        "\n  python XML_Swap_ECU_Name_Desc_Wildcard_v2.py *.xml"
        "\n  python XML_Swap_ECU_Name_Desc_Wildcard_v2.py Merged_*.xml"
    )
    sys.exit(1)

input_files = []

for pattern in sys.argv[1:]:
    matches = glob.glob(pattern)
    if matches:
        input_files.extend(matches)
    else:
        print(f"No files matched: {pattern}")

input_files = sorted(set(Path(f) for f in input_files))

if not input_files:
    print("No XML files found.")
    sys.exit(1)

print(f"Found {len(input_files)} XML file(s)")

for input_xml in input_files:

    if not input_xml.exists():
        print(f"File not found: {input_xml}")
        continue

    output_xml = input_xml.with_name(
        input_xml.stem + '_ECU_Swapped.xml'
    )

    print(f'Processing: {input_xml}')

    try:
        with open(input_xml, 'rb') as f:
            tree = ET.parse(f, parser)
    except Exception as e:
        print(f'Failed to read: {input_xml}')
        print(e)
        continue

    root = tree.getroot()

    swap_count = 0

    for par in root.findall('.//PAR'):

        name_node = par.find('NAME')
        desc_node = par.find('DESC')

        if (
            name_node is None
            or desc_node is None
            or name_node.text is None
            or desc_node.text is None
        ):
            continue

        old_name = name_node.text.strip()
        old_desc = desc_node.text.strip()

        if old_name.startswith('ECU') and old_desc:
            name_node.text = old_desc
            desc_node.text = old_name
            swap_count += 1

    tree.write(
        str(output_xml),
        encoding='ISO-8859-1',
        xml_declaration=True,
        pretty_print=False
    )

    print(f'  Swapped {swap_count} ECU channels')
    print(f'  Output: {output_xml}')
