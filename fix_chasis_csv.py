#!/usr/bin/env python3
"""fix_chasis_csv.py

CLI utility to convert an Excel/CSV file and normalize a chassis column
to avoid scientific notation (e.g., 1.21117E+11 -> 121117000000).

Usage:
    python fix_chasis_csv.py --input input.xlsx --output output.csv --col N_CHASIS

This script is defensive: it preserves textual values with leading zeros and
handles scientific notation using Decimal for precision.
"""
import argparse
from decimal import Decimal, InvalidOperation
import pandas as pd


def normalize_chasis(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        s = str(val).strip()
        if s == "":
            return None
        # preserve textual values with leading zeros
        if s.isdigit() and s.startswith("0"):
            return s
        # scientific notation string e.g. '1.21117E+11'
        if "e" in s.lower():
            try:
                d = Decimal(s)
                plain = format(d, 'f')
                if plain.endswith('.0'):
                    plain = plain[:-2]
                return plain
            except InvalidOperation:
                return s
        # numeric-looking strings or floats
        if s.replace('.', '', 1).isdigit():
            try:
                f = float(s)
                if f.is_integer():
                    return str(int(f))
                return str(f).rstrip('0').rstrip('.')
            except:
                return s
        return s
    except Exception:
        return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", required=True, help="Input .xlsx/.xls/.csv file")
    p.add_argument("--output", "-o", required=True, help="Output CSV file path")
    p.add_argument("--col", "-c", default="N_CHASIS,N_MOTOR", help="Column name(s) to normalize (comma-separated). Default: N_CHASIS,N_MOTOR")
    p.add_argument("--sheet", "-s", default=0, help="Sheet name or index for Excel files (default 0)")
    args = p.parse_args()

    inp = args.input
    out = args.output
    col = args.col

    print(f"Reading {inp} ...")
    if inp.lower().endswith(('.xls', '.xlsx')):
        df = pd.read_excel(inp, sheet_name=args.sheet, engine='openpyxl', dtype=str)
    elif inp.lower().endswith('.csv'):
        df = pd.read_csv(inp, dtype=str, low_memory=False)
    else:
        raise SystemExit("Unsupported input type. Use .xlsx/.xls or .csv")

    cols = [c.strip() for c in col.split(',') if c.strip()]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"Warning: these columns were not found in input and will be skipped: {missing}")
    for c in cols:
        if c in df.columns:
            print(f"Normalizing column '{c}' ...")
            df[c] = df[c].apply(normalize_chasis)

    print(f"Writing CSV to {out} ...")
    # use utf-8-sig so Excel opens correctly on Windows
    df.to_csv(out, index=False, encoding='utf-8-sig')
    print("Done.")


if __name__ == '__main__':
    main()
