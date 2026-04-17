import functools
import re
import pandas as pd
from typing import Union, Dict


def cb_wrapper(func):
    """
    Decorator that resolves every output variable matching the
    `difference_variable` pattern (e.g. 'emission_co2e_*_ippu_foo') and
    accumulates the results per match. Caches the list of columns that
    match each pattern in `CostBenefits._build_caches` and reuses them from
    `self._diff_var_matches` (avoids a regex loop over every SSP column on
    each call).
    """

    @functools.wraps(func)
    def wrapper_decorator(self,
                          cb_orm=None,
                          cb_var_name: Union[str, None] = None,
                          strategy_code_tx: Union[str, None] = None,
                          data_baseline: Union[pd.DataFrame, None] = None,
                          data_tx: Union[pd.DataFrame, None] = None,
                          cb_var_fields: Union[Dict[str, Union[float, int, str]], None] = None):

        if cb_var_name:
            ## Fetch the record from the cache (O(1); previously 2 SQL queries)
            cb_orm = self.get_cb_var_fields(cb_var_name)

            if strategy_code_tx:
                cb_orm.strategy_code_tx = strategy_code_tx

            cb_orm.strategy_code_base = self.strategy_code_base

            print("---------Costs for: {cb_orm.output_variable_name}.".format(cb_orm=cb_orm))

            if cb_orm.tx_table.cost_type == "system_cost":
                print("The variable is evaluated in System Cost")

                if cb_orm.cb_var_group == 'wali_sanitation_cost_factors' or cb_orm.cb_var_group == 'wali_benefit_of_sanitation_cost_factors':
                    cb_orm.cb_function = 'cb_difference_between_two_strategies'

                # Activate the detailed per-sector × per-fuel fuel cost
                # factors. See the equivalent block in
                # `CostBenefits.compute_cost_benefit_from_variable` for the
                # full rationale. The aggregate `cb:enfu:fuel_cost:X:X`
                # (`cb_var_group='enfu_fuel_cost_factors'`, without the
                # `_detail` suffix) is NOT rescued here to avoid double
                # counting with the detail.
                if cb_orm.cb_var_group == 'enfu_fuel_cost_factors_detail':
                    cb_orm.cb_function = 'cb_difference_between_two_strategies'

                if cb_orm.cb_function == "cb:enfu:fuel_cost:X:X":
                    cb_orm.cb_function = 'cb_difference_between_two_strategies'

            elif cb_orm.tx_table.cost_type == "transformation_cost":
                print("The variable is evaluated in Transformation Cost")

                if not self.tx_in_strategy(cb_orm.transformation_code, cb_orm.strategy_code_tx):
                    print("The TX is not present in the strategy")
                    return pd.DataFrame()

        ## Update the record fields if a cb_var_fields dict was received.
        if isinstance(cb_var_fields, dict):
            self.update_cost_factor_register(cb_var_name=cb_var_name,
                                             cb_var_fields=cb_var_fields)

        ## Fast path: use the precomputed matches in self._diff_var_matches.
        ## Fallback: if for some reason the pattern was not cached, compile
        ## the regex once (instead of on every iteration like the old code).
        pattern = cb_orm.difference_variable
        diff_var_list = None
        if hasattr(self, "_diff_var_matches"):
            diff_var_list = self._diff_var_matches.get(pattern)

        if diff_var_list is None:
            rx = re.compile(pattern.replace("*", ".*"))
            diff_var_list = [c for c in self.ssp_list_of_vars if rx.match(c)]
            if hasattr(self, "_diff_var_matches"):
                self._diff_var_matches[pattern] = diff_var_list

        if not diff_var_list:
            print(f'ERROR IN CB_WRAPPER: No variables match : {pattern}')
            return None

        # For each variable that matches, calculate the costs and benefits
        result_tmp = []

        for diff_var_param in diff_var_list:
            cb_orm.diff_var = diff_var_param

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                result = func(self, cb_orm=cb_orm, data_baseline=data_baseline, data_tx=data_tx)
            else:
                result = func(self, cb_orm=cb_orm)
            result_tmp.append(result)

        # If flagged, sum up the variables in value and difference_value columns
        if cb_orm.sum == 1:
            result_tmp = pd.concat(result_tmp, ignore_index=True)
            llaves_gb = ["region", "time_period", "strategy_code", "future_id"]

            results_summarized = result_tmp.groupby(llaves_gb).agg({
                "value": "sum",
                "difference_value": "sum",
                "variable_value_baseline": "sum",
                "variable_value_pathway": "sum",
            }).reset_index()

            results_summarized["difference_variable"] = cb_orm.diff_var
            results_summarized["variable"] = cb_orm.output_variable_name

            return results_summarized.sort_values(["difference_variable", "time_period"])

        if all(elem is None for elem in result_tmp):
            return None

        appended_results = pd.concat(result_tmp, ignore_index=True)
        return appended_results.sort_values(["difference_variable", "time_period"])

    return wrapper_decorator
