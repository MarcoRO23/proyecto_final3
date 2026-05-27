"""
Fix data-shifted parquet files.

Problem: In some years (1997-2009, 2011-2021), the DATA is shifted RIGHT by 1 column:
  - Column 0 (COBERTURA) has ID_ENTIDAD values
  - Column 1 (ID_ENTIDAD) has ID_MUNICIPIO values
  - Column 2 (ID_MUNICIPIO) has ANIO values (year)
  - etc.

This happens because the first column (COBERTURA) got wrapped and stored as row index.

Solution: Detect and fix by moving data LEFT by 1 column, extracting COBERTURA from the last column.
"""

import pandas as pd
import glob
import os
from pathlib import Path

# Expected column order
EXPECTED_COLUMNS = [
    "COBERTURA",
    "ID_ENTIDAD",
    "ID_MUNICIPIO",
    "ANIO",
    "MES",
    "ID_HORA",
    "ID_MINUTO",
    "ID_DIA",
    "DIASEMANA",
    "URBANA",
    "SUBURBANA",
    "TIPACCID",
    "AUTOMOVIL",
    "CAMPASAJ",
    "MICROBUS",
    "PASCAMION",
    "OMNIBUS",
    "TRANVIA",
    "CAMIONETA",
    "CAMION",
    "TRACTOR",
    "FERROCARRI",
    "MOTOCICLET",
    "BICICLETA",
    "OTROVEHIC",
    "CAUSAACCI",
    "CAPAROD",
    "SEXO",
    "ALIENTO",
    "CINTURON",
    "ID_EDAD",
    "CONDMUERTO",
    "CONDHERIDO",
    "PASAMUERTO",
    "PASAHERIDO",
    "PEATMUERTO",
    "PEATHERIDO",
    "CICLMUERTO",
    "CICLHERIDO",
    "OTROMUERTO",
    "OTROHERIDO",
    "NEMUERTO",
    "NEHERIDO",
    "CLASACC",
    "ESTATUS",
]


def clean_column_names(df):
    """Remove BOM and extra characters from column names."""
    df.columns = (
        df.columns.str.replace("ï»¿", "", regex=False)
        .str.replace("﻿", "", regex=False)
        .str.strip()
        .str.upper()
    )
    return df


def is_shifted(df):
    """
    Check if data is shifted right by 1.
    Signs:
    - COBERTURA contains numbers (should be "Municipal"/"Estatal")
    - ID_MUNICIPIO contains years (>1900)
    """
    if "ID_MUNICIPIO" not in df.columns or "COBERTURA" not in df.columns:
        return False

    # Check if ID_MUNICIPIO has year values
    try:
        max_mun = pd.to_numeric(df["ID_MUNICIPIO"], errors="coerce").max()
        if max_mun > 1900:  # Contains years
            return True
    except:
        pass

    # Check if COBERTURA has numbers instead of text
    cobertura_sample = df["COBERTURA"].head(100).astype(str).str.strip()
    if any(cobertura_sample.str.match(r"^\d+$")):  # Contains only digits
        return True

    return False


def fix_shifted_data(df):
    """
    Fix data that's shifted RIGHT by 1 column.

    Process:
    1. Extract COBERTURA from ESTATUS column (last column)
    2. Shift all data LEFT by 1: drop ESTATUS, use shifted columns
    3. Prepend COBERTURA
    """
    # Get COBERTURA from the last column (ESTATUS position has shifted COBERTURA)
    cobertura = df.iloc[:, -1].copy()  # Last column contains original COBERTURA

    # Drop the last column and shift data left
    df_fixed = df.iloc[:, :-1].copy()

    # Assign correct column names (shift left by 1)
    df_fixed.columns = EXPECTED_COLUMNS[1:]  # All except COBERTURA

    # Insert COBERTURA at the beginning
    df_fixed.insert(0, "COBERTURA", cobertura)

    return df_fixed


def fix_parquet_file(file_path, output_dir):
    """Fix a single parquet file and save to output directory."""
    year = (
        os.path.basename(file_path).replace("atus_anual_", "").replace(".parquet", "")
    )
    output_file = os.path.join(output_dir, f"atus_anual_{year}.parquet")

    try:
        # Read the parquet file
        df = pd.read_parquet(file_path)
        original_len = len(df)

        # Clean column names
        df = clean_column_names(df)

        # Check if shifted
        if is_shifted(df):
            print(f"🔧 {year}: DATA SHIFTED - Fixing...")
            df = fix_shifted_data(df)
            status = "FIXED"
        else:
            print(f"✓ {year}: OK - No fix needed")
            status = "OK"

        # Validate the fix
        if "ID_MUNICIPIO" in df.columns:
            max_mun = pd.to_numeric(df["ID_MUNICIPIO"], errors="coerce").max()
            if max_mun > 1900:
                print(f"  ⚠️  WARNING: ID_MUNICIPIO still has large values ({max_mun})")
                status = "PARTIAL"

        # Save to output directory
        df.to_parquet(output_file, index=False)

        return status, year, original_len

    except Exception as e:
        print(f"❌ {year}: ERROR - {str(e)[:100]}")
        return "ERROR", year, 0


def main():
    """Main function to fix all parquet files."""
    input_dir = "/home/mocos/workspace/proyecto_final3/data/parquet"
    output_dir = "/home/mocos/workspace/proyecto_final3/data/parquet_fixed"

    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("🔨 PARQUET DATA SHIFT FIX (RIGHT → LEFT by 1 column)")
    print("=" * 80)
    print(f"\nInput:  {input_dir}")
    print(f"Output: {output_dir}")
    print("\nProblem: COBERTURA column got moved to row index,")
    print("         shifting all data RIGHT by 1 column.")
    print("\nSolution: Move COBERTURA from last column to first,")
    print("          shift all other data LEFT by 1 column.")
    print("\n" + "=" * 80 + "\n")

    # Get all parquet files
    files = sorted(glob.glob(os.path.join(input_dir, "*.parquet")))

    if not files:
        print(f"❌ No parquet files found in {input_dir}")
        return

    print(f"Processing {len(files)} files...\n")

    results = {"FIXED": [], "OK": [], "PARTIAL": [], "ERROR": []}
    total_rows = 0

    for file_path in files:
        status, year, rows = fix_parquet_file(file_path, output_dir)
        results[status].append(year)
        total_rows += rows

    print(f"\n✓ OK (no fix needed):    {len(results['OK'])} files")
    if results["OK"]:
        print(f"  {', '.join(results['OK'])}")

    print(f"\n🔧 FIXED (was shifted):  {len(results['FIXED'])} files")
    if results["FIXED"]:
        print(f"  {', '.join(results['FIXED'][:15])}")
        if len(results["FIXED"]) > 15:
            print(f"  ... and {len(results['FIXED']) - 15} more")

    print(f"\n⚠️  PARTIAL:              {len(results['PARTIAL'])} files")
    if results["PARTIAL"]:
        print(f"  {', '.join(results['PARTIAL'])}")

    print(f"\n❌ ERRORS:               {len(results['ERROR'])} files")
    if results["ERROR"]:
        print(f"  {', '.join(results['ERROR'])}")

    print(f"\nTotal rows processed: {total_rows:,}")
    print(f"\n✅ All files saved to: {output_dir}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
