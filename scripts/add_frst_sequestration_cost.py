"""Register the technical cost of TX:FRST:INCREASE_SEQUESTRATION.

WHY THIS SCRIPT EXISTS
----------------------
`TX:FRST:INCREASE_SEQUESTRATION` was registered in
`attribute_transformation_code` only (level A of
`docs/examples/add_custom_transformations.py`): the package recognises the
transformation and stops warning about it, but **no cost or benefit is ever
computed for it**, because:

  * `compute_technical_cost_for_strategy` only iterates over the
    `transformation_costs` table, and the TX has no row there; and
  * none of the live `cost_factors` (system costs) reacts to it — the TX
    changes the sequestration emission factors
    (`ef_frst_sequestration_*_kt_co2_ha`), not land areas, so the area-driven
    AFOLU factors (`cb:lndu:ecosystem_services:forests:*`) see a zero
    difference, and the only emissions-driven factor (`change_in_emissions`)
    is bound to `cb_strategy_specific_function`, which is not implemented.

Result: any strategy whose only transformation is FRST:INCREASE_SEQUESTRATION
comes out of the pipeline with a cost of exactly zero.

WHAT IT WRITES (idempotent; safe to re-run)
-------------------------------------------
  1) attribute_transformation_code: completes the existing TX row
     (name / id / sector / description).
  2) tx_table:            cb:frst:technical_cost:increase_sequestration:X
  3) transformation_costs: same variable, driven by the *additional*
     sequestration achieved by the transformation.

COST DRIVER
-----------
`difference_variable = 'emission_co2e_co2_frst_sequestration_*'` with
`sum = 1`, i.e. primary + secondary + mangroves added together. Sequestration
is reported as negative emissions (MtCO2e), so a transformation that increases
sequestration produces a negative difference; with a positive multiplier the
resulting `value` is negative, which is the sign convention this package uses
for costs (see `cb:fgtv:technical_cost:*`).

Area is deliberately NOT used as the driver: FRST:INCREASE_SEQUESTRATION
raises the per-hectare sequestration factor without moving land between
classes, so an area-based difference variable would evaluate to ~0 and
reintroduce the very bug this script fixes.

MULTIPLIER
----------
Default: $20 / tCO2e of additional sequestration (2019 USD), a mid-range
unit cost for improved forest management / assisted natural regeneration.
It is a calibration default, not a country-specific estimate — override it
with `--cost-per-tco2e`, or derive it from a per-hectare figure with
`--cost-per-ha-year` + `--delta-ef` (the package documents
$45/ha/year for forest restoration, Fargione et al. 2021 adjusted to LAC,
see docs/cost_factors/lndu_trns_cb.csv):

    $/tCO2e = ($/ha/year) / (additional tCO2/ha/year)

DOUBLE COUNTING
---------------
If a strategy bundles this TX together with transformations that ADD forest
area (TX:LNDU:INC_REFORESTATION, TX:LNDU:DEC_DEFORESTATION, PLUR...), part of
the change in emission_co2e_co2_frst_sequestration_* comes from the extra
hectares, which are already costed per hectare by
cb:lndu:technical_cost:* . To split the driver between the transformations,
add rows to `strategy_interactions` (interaction_name e.g. 'FRST', one row per
competing variable with its `relative_effect` share); `cb_process_interactions`
then scales each contribution. Single-TX strategies need no interaction row.

USAGE
-----
    python scripts/add_frst_sequestration_cost.py
    python scripts/add_frst_sequestration_cost.py --cost-per-tco2e 12.5
    python scripts/add_frst_sequestration_cost.py --cost-per-ha-year 45 --delta-ef 1.2
    python scripts/add_frst_sequestration_cost.py --xlsx /path/to/cb_config_params.xlsx

NOTE ON EXCEL CONFIGS: `load_cb_parameters()` replaces the whole configuration
with the contents of the workbook, so a run that loads an xlsx will NOT see
the rows written to the packaged DB. Pass `--xlsx` to sync the same rows into
that workbook.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "costs_benefits_ssp" / "database" / "backup" / "cb_data.db"

TX_CODE = "TX:FRST:INCREASE_SEQUESTRATION"
TX_NAME = "FRST: Increase forest sequestration"
TX_SECTOR = "AF"
TX_DESCRIPTION = (
    "Increase the CO2 sequestration factor of standing forests (primary, "
    "secondary and mangroves) through improved forest management, assisted "
    "natural regeneration, enrichment planting and fire/grazing control. "
    "Acts on ef_frst_sequestration_*_kt_co2_ha; land areas are unchanged."
)

OUT_COST = "cb:frst:technical_cost:increase_sequestration:X"
DISPLAY_COST = "Technical cost of increasing forest sequestration"

INTERNAL_NOTES_COST = (
    "Unit cost per additional tCO2e sequestered, applied to the change in "
    "emission_co2e_co2_frst_sequestration_* (primary + secondary + mangroves, "
    "sum=1) between the strategy and the baseline. Covers management, "
    "enrichment planting, fire and grazing control, and monitoring. "
    "Area-based drivers are not usable here: the transformation moves the "
    "sequestration factor, not the land-use areas."
)
DISPLAY_NOTES_COST = (
    "Unit cost of improved forest management / assisted natural regeneration "
    "per additional ton of CO2e sequestered (2019 USD). Calibration default; "
    "replace with country-specific costs when available."
)

CB_FUNCTION = "cb_difference_between_two_strategies"
DIFFERENCE_VARIABLE = "emission_co2e_co2_frst_sequestration_*"
MULTIPLIER_UNIT = "$/mtCO2e"
ANNUAL_CHANGE = 1.0
SUM_DIFF_VARS = 1  # add primary + secondary + mangroves

DEFAULT_COST_PER_TCO2E = 20.0


def derive_cost_per_tco2e(cost_per_ha_year: float, delta_ef_t_co2_ha_year: float) -> float:
    """$/tCO2e implied by a per-hectare annual cost and the additional
    sequestration (tCO2/ha/year) the transformation delivers."""
    if delta_ef_t_co2_ha_year <= 0:
        raise ValueError("--delta-ef must be > 0 (additional tCO2/ha/year)")
    return cost_per_ha_year / delta_ef_t_co2_ha_year


def _multiplier(cost_per_tco2e: float) -> float:
    """Convert $/tCO2e to the $/MtCO2e the package expects (emissions are
    reported in millions of tons)."""
    return cost_per_tco2e * 1_000_000.0


def _natural_units(cost_per_tco2e: float) -> str:
    return f"${cost_per_tco2e:g}/ton CO2e additional sequestration"


def _next_transformation_id(cur: sqlite3.Cursor) -> bytes:
    """Next free transformation_id (stored as an 8-byte little-endian int)."""
    max_int = 0
    for (raw,) in cur.execute("SELECT transformation_id FROM attribute_transformation_code"):
        if raw is None:
            continue
        val = int.from_bytes(raw, "little") if isinstance(raw, (bytes, bytearray)) else int(raw)
        max_int = max(max_int, val)
    return (max_int + 1).to_bytes(8, "little")


def _row_exists(cur: sqlite3.Cursor, table: str, key_col: str, key_val: str) -> bool:
    return cur.execute(
        f"SELECT 1 FROM {table} WHERE {key_col} = ?", (key_val,)
    ).fetchone() is not None


def insert_frst_rows(db_path: Path, cost_per_tco2e: float) -> dict[str, int]:
    """Insert/complete the 3 FRST rows. Returns {table: rows written}."""
    written = {"attribute_transformation_code": 0, "tx_table": 0, "transformation_costs": 0}

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        # --- Row 1: attribute_transformation_code (complete it if partial) ---
        row = cur.execute(
            "SELECT transformation, transformation_id, sector, description "
            "FROM attribute_transformation_code WHERE transformation_code = ?",
            (TX_CODE,),
        ).fetchone()

        if row is None:
            cur.execute(
                "INSERT INTO attribute_transformation_code "
                "(transformation_code, transformation, transformation_id, sector, description) "
                "VALUES (?, ?, ?, ?, ?)",
                (TX_CODE, TX_NAME, _next_transformation_id(cur), TX_SECTOR, TX_DESCRIPTION),
            )
            written["attribute_transformation_code"] += 1
            print(f"  + attribute_transformation_code: {TX_CODE}")
        else:
            name, tx_id, sector, description = row
            if not name or tx_id is None or not sector or not description:
                cur.execute(
                    "UPDATE attribute_transformation_code "
                    "SET transformation = ?, transformation_id = ?, sector = ?, description = ? "
                    "WHERE transformation_code = ?",
                    (
                        name or TX_NAME,
                        tx_id if tx_id is not None else _next_transformation_id(cur),
                        sector or TX_SECTOR,
                        description or TX_DESCRIPTION,
                        TX_CODE,
                    ),
                )
                written["attribute_transformation_code"] += 1
                print(f"  ~ attribute_transformation_code: {TX_CODE} completed")
            else:
                print(f"  = attribute_transformation_code: {TX_CODE} already complete")

        # --- Row 2: tx_table ---
        if not _row_exists(cur, "tx_table", "output_variable_name", OUT_COST):
            cur.execute(
                "INSERT INTO tx_table "
                "(output_variable_name, output_display_name, internal_notes, display_notes, cost_type) "
                "VALUES (?, ?, ?, ?, ?)",
                (OUT_COST, DISPLAY_COST, INTERNAL_NOTES_COST, DISPLAY_NOTES_COST, "transformation_cost"),
            )
            written["tx_table"] += 1
            print(f"  + tx_table: {OUT_COST}")
        else:
            print(f"  = tx_table: {OUT_COST} already present")

        # --- Row 3: transformation_costs ---
        multiplier = _multiplier(cost_per_tco2e)
        if not _row_exists(cur, "transformation_costs", "output_variable_name", OUT_COST):
            cur.execute(
                "INSERT INTO transformation_costs "
                "(output_variable_name, transformation_code, include, include_variant, "
                " test_id_variant_suffix, comparison_id_variant, cb_function, difference_variable, "
                " multiplier, multiplier_unit, annual_change, arg1, arg2, sum, natural_multiplier_units) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    OUT_COST, TX_CODE, 1, 99.0, "ND", "ND", CB_FUNCTION, DIFFERENCE_VARIABLE,
                    multiplier, MULTIPLIER_UNIT, ANNUAL_CHANGE, "ND", 99.0, SUM_DIFF_VARS,
                    _natural_units(cost_per_tco2e),
                ),
            )
            written["transformation_costs"] += 1
            print(f"  + transformation_costs: {OUT_COST} (mult={multiplier:+,.0f})")
        else:
            cur.execute(
                "UPDATE transformation_costs "
                "SET multiplier = ?, natural_multiplier_units = ? WHERE output_variable_name = ?",
                (multiplier, _natural_units(cost_per_tco2e), OUT_COST),
            )
            print(f"  = transformation_costs: {OUT_COST} present (multiplier set to {multiplier:+,.0f})")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return written


def sync_to_xlsx(xlsx_path: Path, cost_per_tco2e: float) -> dict[str, int]:
    """Append the same rows to a cb_config_params.xlsx workbook. Idempotent."""
    import pandas as pd  # imported lazily: only needed for the xlsx path

    if not xlsx_path.exists():
        raise SystemExit(f"xlsx not found: {xlsx_path}")

    all_sheets = pd.read_excel(xlsx_path, sheet_name=None)
    inserted = {"attribute_transformation_code": 0, "tx_table": 0, "transformation_costs": 0}

    sheet = all_sheets["attribute_transformation_code"]
    if TX_CODE not in sheet["transformation_code"].astype(str).values:
        ids = pd.to_numeric(sheet["transformation_id"], errors="coerce")
        next_id = int(ids.max()) + 1 if ids.notna().any() else 1
        all_sheets["attribute_transformation_code"] = pd.concat(
            [sheet, pd.DataFrame([{
                "transformation_code": TX_CODE,
                "transformation": TX_NAME,
                "transformation_id": next_id,
                "sector": TX_SECTOR,
                "description": TX_DESCRIPTION,
            }])], ignore_index=True,
        )
        inserted["attribute_transformation_code"] += 1
        print(f"  + xlsx attribute_transformation_code: {TX_CODE}")
    else:
        print(f"  = xlsx attribute_transformation_code: {TX_CODE} already present")

    sheet = all_sheets["tx_table"]
    if OUT_COST not in sheet["output_variable_name"].astype(str).values:
        all_sheets["tx_table"] = pd.concat(
            [sheet, pd.DataFrame([{
                "output_variable_name": OUT_COST,
                "output_display_name": DISPLAY_COST,
                "internal_notes": INTERNAL_NOTES_COST,
                "display_notes": DISPLAY_NOTES_COST,
                "cost_type": "transformation_cost",
            }])], ignore_index=True,
        )
        inserted["tx_table"] += 1
        print(f"  + xlsx tx_table: {OUT_COST}")
    else:
        print(f"  = xlsx tx_table: {OUT_COST} already present")

    sheet = all_sheets["transformation_costs"]
    if OUT_COST not in sheet["output_variable_name"].astype(str).values:
        all_sheets["transformation_costs"] = pd.concat(
            [sheet, pd.DataFrame([{
                "output_variable_name": OUT_COST,
                "transformation_code": TX_CODE,
                "include": True,
                "include_variant": 99,
                "test_id_variant_suffix": "ND",
                "comparison_id_variant": "ND",
                "cb_function": CB_FUNCTION,
                "difference_variable": DIFFERENCE_VARIABLE,
                "multiplier": _multiplier(cost_per_tco2e),
                "multiplier_unit": MULTIPLIER_UNIT,
                "annual_change": ANNUAL_CHANGE,
                "arg1": "ND",
                "arg2": 99,
                "sum": bool(SUM_DIFF_VARS),
                "natural_multiplier_units": _natural_units(cost_per_tco2e),
            }])], ignore_index=True,
        )
        inserted["transformation_costs"] += 1
        print(f"  + xlsx transformation_costs: {OUT_COST}")
    else:
        print(f"  = xlsx transformation_costs: {OUT_COST} already present")

    if sum(inserted.values()) > 0:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl", mode="w") as writer:
            for sheet_name, df in all_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"\n  Wrote updates to {xlsx_path}")
    else:
        print("\n  xlsx already up to date.")

    return inserted


def verify(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    print("\n== verification ==")
    print("\nattribute_transformation_code (FRST):")
    for row in cur.execute(
        "SELECT transformation_code, transformation, sector, length(description) "
        "FROM attribute_transformation_code WHERE transformation_code = ?",
        (TX_CODE,),
    ):
        print(" ", row)

    print("\ntx_table (FRST):")
    for row in cur.execute(
        "SELECT output_variable_name, output_display_name, cost_type "
        "FROM tx_table WHERE output_variable_name = ?",
        (OUT_COST,),
    ):
        print(" ", row)

    print("\ntransformation_costs (FRST):")
    for row in cur.execute(
        "SELECT output_variable_name, transformation_code, cb_function, difference_variable, "
        "       multiplier, multiplier_unit, sum, natural_multiplier_units "
        "FROM transformation_costs WHERE transformation_code = ?",
        (TX_CODE,),
    ):
        print(" ", row)
    conn.close()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", type=Path, default=DB_PATH, help="target cb_data.db (default: packaged DB)")
    p.add_argument("--xlsx", type=Path, default=None,
                   help="also sync the rows into this cb_config_params.xlsx")
    p.add_argument("--cost-per-tco2e", type=float, default=None,
                   help=f"unit cost in $/tCO2e (default: {DEFAULT_COST_PER_TCO2E})")
    p.add_argument("--cost-per-ha-year", type=float, default=None,
                   help="alternative: annual cost per hectare under improved management")
    p.add_argument("--delta-ef", type=float, default=None,
                   help="alternative: additional tCO2/ha/year delivered by the transformation")
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    if args.cost_per_ha_year is not None or args.delta_ef is not None:
        if args.cost_per_ha_year is None or args.delta_ef is None:
            raise SystemExit("--cost-per-ha-year and --delta-ef must be given together")
        if args.cost_per_tco2e is not None:
            raise SystemExit("use either --cost-per-tco2e or --cost-per-ha-year/--delta-ef, not both")
        cost_per_tco2e = derive_cost_per_tco2e(args.cost_per_ha_year, args.delta_ef)
        print(f"Derived unit cost: ${cost_per_tco2e:,.2f}/tCO2e "
              f"(= ${args.cost_per_ha_year:g}/ha/yr / {args.delta_ef:g} tCO2/ha/yr)")
    else:
        cost_per_tco2e = args.cost_per_tco2e if args.cost_per_tco2e is not None else DEFAULT_COST_PER_TCO2E

    print(f"Target DB: {args.db}")
    if not args.db.exists():
        raise SystemExit(f"DB not found: {args.db}")

    print(f"\nWriting FRST sequestration cost rows (idempotent, ${cost_per_tco2e:g}/tCO2e)...")
    result = insert_frst_rows(args.db, cost_per_tco2e)
    print(f"\nRows written: {result} (total={sum(result.values())})")
    verify(args.db)

    if args.xlsx is not None:
        print(f"\nSyncing to workbook: {args.xlsx}")
        sync_to_xlsx(args.xlsx, cost_per_tco2e)

    print("\nDone.")


if __name__ == "__main__":
    main()
