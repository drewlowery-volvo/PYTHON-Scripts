
#!/usr/bin/env python3
"""
clean_dvf_channels.py

Remove channels from .dvf/.mat files that contain no useful transient data.

Rules (configurable in code):
- Remove channels that are all zeros.
- Remove channels that are all -9999.
- Remove channels with only a single repeated value.
- Remove channels with too few unique values.
- Keep time-like channels if requested.

Usage:
    python clean_dvf_channels.py input.dvf output.dvf
    python clean_dvf_channels.py C:\\Data --recursive
"""

from pathlib import Path
import argparse
import numpy as np
from scipy.io import loadmat, savemat

BAD_VALUES = {-9999, -999, 9999}
MIN_UNIQUE_VALUES = 3


def is_useless_channel(arr):
    try:
        x = np.asarray(arr).squeeze()

        if x.size == 0:
            return True, 'empty'

        if not np.issubdtype(x.dtype, np.number):
            return False, 'non-numeric'

        finite = x[np.isfinite(x)]
        if finite.size == 0:
            return True, 'all_nan'

        if np.all(finite == 0):
            return True, 'all_zero'

        for bad in BAD_VALUES:
            if np.all(finite == bad):
                return True, f'all_{bad}'

        unique_vals = np.unique(finite)

        if unique_vals.size == 1:
            return True, 'constant'

        if unique_vals.size < MIN_UNIQUE_VALUES:
            return True, 'low_variation'

        return False, 'valid'

    except Exception as exc:
        return False, f'error:{exc}'


def process_file(infile, outfile=None):
    data = loadmat(infile, squeeze_me=True, struct_as_record=False)

    cleaned = {}
    removed = []

    for key, value in data.items():
        if key.startswith('__'):
            cleaned[key] = value
            continue

        remove, reason = is_useless_channel(value)

        if remove:
            removed.append((key, reason))
        else:
            cleaned[key] = value

    if outfile:
        savemat(outfile, cleaned, do_compression=True)

    return removed, len(cleaned)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    parser.add_argument('output', nargs='?')
    parser.add_argument('--recursive', action='store_true')
    args = parser.parse_args()

    p = Path(args.path)

    if p.is_file():
        outfile = args.output or str(p.with_name(p.stem + '_cleaned.dvf'))
        removed, kept = process_file(p, outfile)

        print(f'Processed: {p}')
        print(f'Kept channels: {kept}')
        print(f'Removed channels: {len(removed)}')
        for ch, reason in removed:
            print(f'  {ch}: {reason}')

    else:
        pattern = '**/*.dvf' if args.recursive else '*.dvf'
        for f in p.glob(pattern):
            out = f.with_name(f.stem + '_cleaned.dvf')
            removed, kept = process_file(f, out)
            print(f'{f.name}: removed {len(removed)} channels')


if __name__ == '__main__':
    main()
