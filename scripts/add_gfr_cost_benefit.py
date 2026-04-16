"""Register cost and benefit entries for TX:FGTV:INC_GAS_RECOVERY.

Idempotent: safe to re-run; existing rows are left untouched.

Writes 5 rows across 3 tables:
  1) attribute_transformation_code: TX:FGTV:INC_GAS_RECOVERY
  2) tx_table: cb:fgtv:technical_cost:gas_recovery:X
  3) tx_table: cb:fgtv:technical_savings:gas_recovery:X
  4) transformation_costs: cb:fgtv:technical_cost:gas_recovery:X  (multiplier = +14.5 M $/MtCO2e)
  5) transformation_costs: cb:fgtv:technical_savings:gas_recovery:X (multiplier = -2.5 M $/MtCO2e, placeholder)

Cost multiplier is derived from Libya NDC dossier Sept 2025 (slides 11 & 28):
    $1,233 M cumulative GFR CAPEX / 85 MtCO2e cumulative 2026-2035 abatement
    = $14.5 / tCO2e (2019 USD)

Benefit multiplier is a conservative placeholder ($2.5 / tCO2e recovered-gas value),
to be calibrated against regional natural-gas benchmark prices when needed.

Dispatcher: cb_difference_between_two_strategies (matches Libya xlsx siblings).
See plan: .claude/plans/tingly-jumping-cherny.md (in sisepuede worktree).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "costs_benefits_ssp" / "database" / "backup" / "cb_data.db"

TX_CODE = "TX:FGTV:INC_GAS_RECOVERY"
TX_NAME = "FGTV: Increase gas recovery"
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

# Dispatcher: matches the pattern used by the Libya xlsx for DEC_LEAKS / INC_FLARE.
CB_FUNCTION = "cb_difference_between_two_strategies"
DIFFERENCE_VARIABLE = "emission_co2e_subsector_total_fgtv"
MULTIPLIER_UNIT = "$/mtCO2e"
ANNUAL_CHANGE = 1.0

MULTIPLIER_COST = 14_500_000.0  # +$14.5 / tCO2e
NATURAL_UNITS_COST = "$14.5/ton CO2e"
MULTIPLIER_BENEFIT = -2_500_000.0  # placeholder -$2.5 / tCO2e
NATURAL_UNITS_BENEFIT = "-$2.5/ton CO2e"


def _next_transformation_id(cur: sqlite3.Cursor) -> bytes:
    """Get the next free transformation_id (stored as 8-byte little-endian int)."""
    row = cur.execute(
        "SELECT transformation_id FROM attribute_transformation_code"
    ).fetchall()
    max_int = 0
    for (raw,) in row:
        if raw is None:
            continue
        val = int.from_bytes(raw, "little")
        if val > max_int:
            max_int = val
    return (max_int + 1).to_bytes(8, "little")


def _row_exists(cur: sqlite3.Cursor, table: str, key_col: str, key_val: str) -> bool:
    return cur.execute(
        f"SELECT 1 FROM {table} WHERE {key_col} = ?", (key_val,)
    ).fetchone() is not None


def insert_gfr_rows(db_path: Path) -> dict[str, int]:
    """Insert the 5 GFR rows. Returns a dict {table: rows_inserted}."""
    inserted = {"attribute_transformation_code": 0, "tx_table": 0, "transformation_costs": 0}

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        # --- Row 1: attribute_transformation_code ---
        if not _row_exists(cur, "attribute_transformation_code", "transformation_code", TX_CODE):
            new_id = _next_transformation_id(cur)
            cur.execute(
                "INSERT INTO attribute_transformation_code "
                "(transformation_code, transformation, transformation_id, sector, description) "
                "VALUES (?, ?, ?, ?, ?)",
                (TX_CODE, TX_NAME, new_id, "EN", TX_DESCRIPTION),
            )
            inserted["attribute_transformation_code"] += 1
            print(f"  + attribute_transformation_code: {TX_CODE} (id={int.from_bytes(new_id, 'little')})")
        else:
            print(f"  = attribute_transformation_code: {TX_CODE} already present")

        # --- Rows 2 & 3: tx_table ---
        for out_var, display, internal, display_notes in [
            (OUT_COST, DISPLAY_COST, INTERNAL_NOTES_COST, DISPLAY_NOTES_COST),
            (OUT_BENEFIT, DISPLAY_BENEFIT, INTERNAL_NOTES_BENEFIT, DISPLAY_NOTES_BENEFIT),
        ]:
            if not _row_exists(cur, "tx_table", "output_variable_name", out_var):
                cur.execute(
                    "INSERT INTO tx_table "
                    "(output_variable_name, output_display_name, internal_notes, display_notes, cost_type) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (out_var, display, internal, display_notes, "transformation_cost"),
                )
                inserted["tx_table"] += 1
                print(f"  + tx_table: {out_var}")
            else:
                print(f"  = tx_table: {out_var} already present")

        # --- Rows 4 & 5: transformation_costs ---
        common_cost_row = {
            "transformation_code": TX_CODE,
            "include": 1,
            "include_variant": 99.0,
            "test_id_variant_suffix": "ND",
            "comparison_id_variant": "ND",
            "cb_function": CB_FUNCTION,
            "difference_variable": DIFFERENCE_VARIABLE,
            "multiplier_unit": MULTIPLIER_UNIT,
            "annual_change": ANNUAL_CHANGE,
            "arg1": "ND",
            "arg2": 99.0,
            "sum": 0,
        }
        for out_var, mult, nat_units in [
            (OUT_COST, MULTIPLIER_COST, NATURAL_UNITS_COST),
            (OUT_BENEFIT, MULTIPLIER_BENEFIT, NATURAL_UNITS_BENEFIT),
        ]:
            if not _row_exists(cur, "transformation_costs", "output_variable_name", out_var):
                cur.execute(
                    "INSERT INTO transformation_costs "
                    "(output_variable_name, transformation_code, include, include_variant, "
                    " test_id_variant_suffix, comparison_id_variant, cb_function, difference_variable, "
                    " multiplier, multiplier_unit, annual_change, arg1, arg2, sum, natural_multiplier_units) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        out_var,
                        common_cost_row["transformation_code"],
                        common_cost_row["include"],
                        common_cost_row["include_variant"],
                        common_cost_row["test_id_variant_suffix"],
                        common_cost_row["comparison_id_variant"],
                        common_cost_row["cb_function"],
                        common_cost_row["difference_variable"],
                        mult,
                        common_cost_row["multiplier_unit"],
                        common_cost_row["annual_change"],
                        common_cost_row["arg1"],
                        common_cost_row["arg2"],
                        common_cost_row["sum"],
                        nat_units,
                    ),
                )
                inserted["transformation_costs"] += 1
                print(f"  + transformation_costs: {out_var} (mult={mult:+,.0f})")
            else:
                print(f"  = transformation_costs: {out_var} already present")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return inserted


def verify(db_path: Path) -> None:
    """Print the new rows to confirm insertion."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    print("\n== verification ==")
    print("\nattribute_transformation_code (GFR):")
    for row in cur.execute(
        "SELECT transformation_code, transformation, sector, "
        "       CAST(transformation_id AS INTEGER), length(description) "
        "FROM attribute_transformation_code WHERE transformation_code = ?",
        (TX_CODE,),
    ):
        print(" ", row)

    print("\ntx_table (GFR):")
    for row in cur.execute(
        "SELECT output_variable_name, output_display_name, cost_type "
        "FROM tx_table WHERE output_variable_name LIKE 'cb:fgtv:%:gas_recovery:%'"
    ):
        print(" ", row)

    print("\ntransformation_costs (GFR):")
    for row in cur.execute(
        "SELECT output_variable_name, transformation_code, cb_function, "
        "       multiplier, multiplier_unit, natural_multiplier_units "
        "FROM transformation_costs WHERE transformation_code = ?",
        (TX_CODE,),
    ):
        print(" ", row)

    conn.close()


if __name__ == "__main__":
    print(f"Target DB: {DB_PATH}")
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    print("\nInserting GFR cost + benefit rows (idempotent)...")
    result = insert_gfr_rows(DB_PATH)

    total = sum(result.values())
    print(f"\nInserted rows: {result} (total={total})")
    verify(DB_PATH)
    print("\nDone.")
