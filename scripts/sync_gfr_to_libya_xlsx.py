"""Sync the 5 GFR rows into Libya's local cb_config_params.xlsx.

Reads the current Libya config, appends the GFR rows if missing, writes back.
Idempotent.

Mirrors the rows inserted by scripts/add_gfr_cost_benefit.py; uses the same
cb_function / difference_variable / multipliers so the Libya CBA run matches
what the upstream DB produces.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

LIBYA_XLSX = Path(
    "/Users/fabianfuentes/git/ssp_libya/ssp_modeling/cost-benefits/"
    "cb_config_files/cb_config_params.xlsx"
)

TX_CODE = "TX:FGTV:INC_GAS_RECOVERY"
TX_NAME = "FGTV: Increase gas recovery"
TX_ID = 68  # matches upstream DB insertion
TX_DESCRIPTION = (
    "Capture associated gas at the wellhead (LP compressors, thermal oxidizers, "
    "re-injection, or productive use) and remove it from flaring + venting + "
    "fugitive streams proportionally to the capture fraction."
)

OUT_COST = "cb:fgtv:technical_cost:gas_recovery:X"
OUT_BENEFIT = "cb:fgtv:technical_savings:gas_recovery:X"

DISPLAY_COST = "Technical cost of gas flaring recovery (GFR)"
DISPLAY_BENEFIT = "Revenue from recovered associated gas (GFR)"

INTERNAL_NOTES_COST = (
    "Cumulative CAPEX for wellhead gas capture infrastructure (compressors, "
    "oxidizers, re-injection). Per-ton CAPEX derived from Libya NDC dossier 2025 "
    "($1,233 M / 85 MtCO2e ~= $14.5/tCO2e)."
)
DISPLAY_NOTES_COST = "Libya NDC Workshop Sept 2025, slides 11 & 28. 2019 USD."

INTERNAL_NOTES_BENEFIT = (
    "Avoided-gas monetization: recovered associated gas replaces marginal supply "
    "(re-injection / export / electricity feedstock). Multiplier expressed as "
    "negative $/MtCO2e. Placeholder -$2.5/tCO2e; calibrate per region."
)
DISPLAY_NOTES_BENEFIT = (
    "Gas-sales benefit, value bounded by regional Henry Hub / LNG benchmark "
    "(2019 USD)."
)

CB_FUNCTION = "cb_difference_between_two_strategies"
DIFFERENCE_VARIABLE = "emission_co2e_subsector_total_fgtv"
MULTIPLIER_UNIT = "$/mtCO2e"
MULTIPLIER_COST = 14_500_000.0
NATURAL_UNITS_COST = "$14.5/ton CO2e"
MULTIPLIER_BENEFIT = -2_500_000.0
NATURAL_UNITS_BENEFIT = "-$2.5/ton CO2e"


def _row_attr_tx_code() -> dict:
    return {
        "transformation_code": TX_CODE,
        "transformation": TX_NAME,
        "transformation_id": TX_ID,
        "sector": "EN",
        "description": TX_DESCRIPTION,
    }


def _row_tx_table(out_var: str, display: str, internal: str, display_notes: str) -> dict:
    return {
        "output_variable_name": out_var,
        "output_display_name": display,
        "internal_notes": internal,
        "display_notes": display_notes,
        "cost_type": "transformation_cost",
    }


def _row_transformation_costs(out_var: str, multiplier: float, natural_units: str) -> dict:
    return {
        "output_variable_name": out_var,
        "transformation_code": TX_CODE,
        "include": True,
        "include_variant": 99,
        "test_id_variant_suffix": "ND",
        "comparison_id_variant": "ND",
        "cb_function": CB_FUNCTION,
        "difference_variable": DIFFERENCE_VARIABLE,
        "multiplier": multiplier,
        "multiplier_unit": MULTIPLIER_UNIT,
        "annual_change": 1.0,
        "arg1": "ND",
        "arg2": 99,
        "sum": False,
        "natural_multiplier_units": natural_units,
    }


def sync_libya_xlsx(xlsx_path: Path) -> dict[str, int]:
    """Append missing GFR rows into the 3 relevant sheets. Returns counts inserted."""
    if not xlsx_path.exists():
        raise SystemExit(f"Libya xlsx not found: {xlsx_path}")

    # Read all sheets preserving them
    all_sheets = pd.read_excel(xlsx_path, sheet_name=None)

    inserted = {
        "attribute_transformation_code": 0,
        "tx_table": 0,
        "transformation_costs": 0,
    }

    # --- attribute_transformation_code ---
    sheet = all_sheets["attribute_transformation_code"]
    if TX_CODE not in sheet["transformation_code"].astype(str).values:
        new_row = _row_attr_tx_code()
        all_sheets["attribute_transformation_code"] = pd.concat(
            [sheet, pd.DataFrame([new_row])], ignore_index=True
        )
        inserted["attribute_transformation_code"] += 1
        print(f"  + attribute_transformation_code: {TX_CODE}")
    else:
        print(f"  = attribute_transformation_code: {TX_CODE} already present")

    # --- tx_table ---
    sheet = all_sheets["tx_table"]
    for out_var, disp, internal, disp_notes in [
        (OUT_COST, DISPLAY_COST, INTERNAL_NOTES_COST, DISPLAY_NOTES_COST),
        (OUT_BENEFIT, DISPLAY_BENEFIT, INTERNAL_NOTES_BENEFIT, DISPLAY_NOTES_BENEFIT),
    ]:
        if out_var not in sheet["output_variable_name"].astype(str).values:
            new_row = _row_tx_table(out_var, disp, internal, disp_notes)
            sheet = pd.concat([sheet, pd.DataFrame([new_row])], ignore_index=True)
            inserted["tx_table"] += 1
            print(f"  + tx_table: {out_var}")
        else:
            print(f"  = tx_table: {out_var} already present")
    all_sheets["tx_table"] = sheet

    # --- transformation_costs ---
    sheet = all_sheets["transformation_costs"]
    for out_var, mult, nat in [
        (OUT_COST, MULTIPLIER_COST, NATURAL_UNITS_COST),
        (OUT_BENEFIT, MULTIPLIER_BENEFIT, NATURAL_UNITS_BENEFIT),
    ]:
        if out_var not in sheet["output_variable_name"].astype(str).values:
            new_row = _row_transformation_costs(out_var, mult, nat)
            sheet = pd.concat([sheet, pd.DataFrame([new_row])], ignore_index=True)
            inserted["transformation_costs"] += 1
            print(f"  + transformation_costs: {out_var} (mult={mult:+,.0f})")
        else:
            print(f"  = transformation_costs: {out_var} already present")
    all_sheets["transformation_costs"] = sheet

    # Write back, preserving all sheets
    if sum(inserted.values()) > 0:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl", mode="w") as writer:
            for sheet_name, df in all_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"\n  Wrote updates to {xlsx_path}")
    else:
        print("\n  No changes needed (all rows already present).")

    return inserted


def verify(xlsx_path: Path) -> None:
    print("\n== verification ==")
    for sheet_name, key_col, filter_val in [
        ("attribute_transformation_code", "transformation_code", TX_CODE),
        ("tx_table", "output_variable_name", "cb:fgtv:%:gas_recovery:X"),
        ("transformation_costs", "transformation_code", TX_CODE),
    ]:
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
        if "%" in filter_val:
            mask = df[key_col].astype(str).str.contains("gas_recovery")
        else:
            mask = df[key_col].astype(str) == filter_val
        print(f"\n{sheet_name}:")
        print(df[mask].to_string(index=False))


if __name__ == "__main__":
    print(f"Target xlsx: {LIBYA_XLSX}")
    result = sync_libya_xlsx(LIBYA_XLSX)
    total = sum(result.values())
    print(f"\nInserted rows: {result} (total={total})")
    verify(LIBYA_XLSX)
    print("\nDone.")
