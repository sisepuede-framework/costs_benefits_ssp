"""
Script de benchmark y regresión para el paquete costs_benefits_ssp.

Uso:
  python bench_optimization.py         # corre el pipeline completo y mide tiempos
  python bench_optimization.py --save  # adicionalmente guarda el resultado en CSV

La idea es tener UNA misma salida determinista que se pueda comparar antes y
después del refactor: el DataFrame final `results_all` se ordena y se
serializa, y se puede diff'ear.
"""
import argparse
import os
import time
import hashlib

import pandas as pd

from costs_benefits_ssp.cb_calculate import CostBenefits

HERE = os.path.dirname(os.path.abspath(__file__))
TEST_DATA = os.path.join(HERE, "test_data")

SSP_FILE = os.path.join(
    TEST_DATA,
    "sisepuede_results_sisepuede_run_2025-02-11T11;37;41.739098_WIDE_INPUTS_OUTPUTS.csv",
)
ATT_PRIMARY = os.path.join(TEST_DATA, "ATTRIBUTE_PRIMARY.csv")
ATT_STRATEGY = os.path.join(TEST_DATA, "ATTRIBUTE_STRATEGY.csv")


def hash_df(df: pd.DataFrame) -> str:
    """Hash determinista del DataFrame ordenado para comparar semánticamente."""
    sort_cols = [c for c in df.columns if c != "value"]
    df_sorted = df.sort_values(sort_cols).reset_index(drop=True)
    raw = pd.util.hash_pandas_object(df_sorted, index=False).values.tobytes()
    return hashlib.sha256(raw).hexdigest()[:16]


def main(save: bool = False) -> None:
    t_all = time.perf_counter()

    print("Cargando datos de entrada ...")
    t0 = time.perf_counter()
    ssp_data = pd.read_csv(SSP_FILE)
    att_primary = pd.read_csv(ATT_PRIMARY)
    att_strategy = pd.read_csv(ATT_STRATEGY)
    print(f"  datos cargados en {time.perf_counter() - t0:.2f}s")

    print("Instanciando CostBenefits ...")
    t0 = time.perf_counter()
    cb = CostBenefits(ssp_data, att_primary, att_strategy, "BASE")
    print(f"  init en {time.perf_counter() - t0:.2f}s")

    print("compute_system_cost_for_all_strategies() ...")
    t0 = time.perf_counter()
    results_system = cb.compute_system_cost_for_all_strategies(verbose=False)
    t_sys = time.perf_counter() - t0
    print(f"  system costs en {t_sys:.2f}s  ({len(results_system)} filas)")

    print("compute_technical_cost_for_all_strategies() ...")
    t0 = time.perf_counter()
    results_tx = cb.compute_technical_cost_for_all_strategies(verbose=False)
    t_tx = time.perf_counter() - t0
    print(f"  technical costs en {t_tx:.2f}s  ({len(results_tx)} filas)")

    results_all = pd.concat([results_system, results_tx], ignore_index=True)

    print("cb_process_interactions() ...")
    t0 = time.perf_counter()
    results_all_pp = cb.cb_process_interactions(results_all)
    t_int = time.perf_counter() - t0
    print(f"  interactions en {t_int:.2f}s  ({len(results_all_pp)} filas)")

    print("cb_shift_costs() ...")
    t0 = time.perf_counter()
    results_final = cb.cb_shift_costs(results_all_pp)
    t_shift = time.perf_counter() - t0
    print(f"  shift costs en {t_shift:.2f}s  ({len(results_final)} filas)")

    t_total = time.perf_counter() - t_all
    print("\n======= RESUMEN =======")
    print(f"system:       {t_sys:8.2f}s")
    print(f"technical:    {t_tx:8.2f}s")
    print(f"interactions: {t_int:8.2f}s")
    print(f"shift:        {t_shift:8.2f}s")
    print(f"TOTAL:        {t_total:8.2f}s")

    h_sys = hash_df(results_system)
    h_tx = hash_df(results_tx)
    h_pp = hash_df(results_all_pp)
    print("\n======= HASHES =======")
    print(f"system       hash: {h_sys}")
    print(f"technical    hash: {h_tx}")
    print(f"interactions hash: {h_pp}")

    if save:
        tag = os.environ.get("BENCH_TAG", "run")
        out_sys = os.path.join(HERE, f"bench_system_{tag}.csv")
        out_tx = os.path.join(HERE, f"bench_technical_{tag}.csv")
        out_int = os.path.join(HERE, f"bench_interactions_{tag}.csv")
        results_system.sort_values(
            [c for c in results_system.columns if c != "value"]
        ).reset_index(drop=True).to_csv(out_sys, index=False)
        results_tx.sort_values(
            [c for c in results_tx.columns if c != "value"]
        ).reset_index(drop=True).to_csv(out_tx, index=False)
        results_all_pp.sort_values(
            [c for c in results_all_pp.columns if c != "value"]
        ).reset_index(drop=True).to_csv(out_int, index=False)
        print(f"\nResultados guardados: {tag}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()
    main(save=args.save)
