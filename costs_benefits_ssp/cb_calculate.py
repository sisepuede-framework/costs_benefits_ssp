from typing import List, Union, Dict, Callable, Any
from sqlalchemy.orm import Session
import pandas as pd

import warnings

import logging

import numpy as np
import re

import os
import shutil

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import load_only
from sqlalchemy.orm import declarative_base

from costs_benefits_ssp.utils.utils import build_path,get_tx_prefix
from costs_benefits_ssp.model.cb_data_model import TXTable,CostFactor,TransformationCost,StrategyInteraction
from costs_benefits_ssp.decorators.cb_wrappers import cb_wrapper 
from costs_benefits_ssp.config.cb_config import * 

from costs_benefits_ssp.model.cb_data_model import CountriesISO,AttDimTimePeriod,AttTransformationCode

from costs_benefits_ssp.model.cb_data_model import (AgrcLVSTProductivityCostGDP,AgrcRiceMGMTTX,ENTCReduceLosses,
                                                    IPPUCCSCostFactor,IPPUFgasDesignation,LNDUSoilCarbonFraction,
                                                    LVSTEntericFermentationTX,LVSTTLUConversion,PFLOTransitionNewDiets,
                                                    WALISanitationClassificationSP)

from costs_benefits_ssp.model.cb_update_data_model import update_db_schema
from sqlalchemy import update

class CostBenefits:
    """
    Class that loads the files defined in the Enums.

    Initialization arguments
    -----------------------------

    - data_file_path : directory where the input data is stored


    Optional arguments
    -----------------------------
    - logger : optional logger instance to track file-loading events

    """
    def __init__(self, 
                 ssp_data : pd.DataFrame,
                 att_primary : pd.DataFrame,
                 att_strategy : pd.DataFrame,
                 strategy_code_base : str,
                 logger: Union[logging.Logger, None] = None
                 ) -> None:

        self.session = self.initialize_session()
        self.strategy_to_txs : Dict[str, List[str]] = self.get_strategy_to_txs(att_strategy)
        self.att_strategy = att_strategy
        self.strategy_code_base = strategy_code_base
        self.ssp_data = self.marge_attribute_strategy(ssp_data, att_primary, att_strategy)
        self.ssp_list_of_vars = list(self.ssp_data)
        self.ssp_data = self.add_additional_columns()
        self.ssp_list_of_vars = list(self.ssp_data)
        # Índice por columna para acceso O(1) (usado por varias optimizaciones).
        self._ssp_col_set = set(self.ssp_list_of_vars)

        # In-memory caches of every configuration table plus per-strategy
        # views of `ssp_data`. These replace the SQL reads and per-call
        # copies that the original code performed inside every `cb_*`
        # function.
        self._build_caches()
        self._build_strategy_views()


    ##############################################
	#------ METODOS DE INICIALIZACION	   ------#
	##############################################

    def marge_attribute_strategy(
                 self,
                 ssp_data : pd.DataFrame,
                 att_primary : pd.DataFrame,
                 att_strategy : pd.DataFrame,      
                ) -> pd.DataFrame:
        
        merged_attributes = att_primary.merge(right = att_strategy, on = "strategy_id")
        return merged_attributes[['primary_id', 'strategy_code', 'future_id']].merge(right = ssp_data, on='primary_id')

        

    def initialize_session(
                self
                ) -> Session:
        
        FILE_PATH = os.path.dirname(os.path.abspath(__file__))

        # Source DB path
        DB_FILE_PATH = build_path([FILE_PATH, "database", "backup", "cb_data.db"])
        
        # Destination DB path
        DB_TMP_FILE_PATH = build_path([FILE_PATH, "database", "tmp_cb_data.db"])
        
        # Copy tmp DB file
        shutil.copyfile(DB_FILE_PATH, DB_TMP_FILE_PATH)

        # Create engine
        engine = create_engine(f"sqlite:///{DB_TMP_FILE_PATH}")
        
        # Create Session
        Session = sessionmaker(bind=engine)

        return Session()


    def get_strategy_to_txs(
                self,
                att_strategy : pd.DataFrame,
        ) -> Dict[str, List[str]]:

        # Get the list of transformations supported by the package.
        ssp_txs = [i.transformation_code for i in self.session.query(AttTransformationCode).all()]

        original_strategy_to_txs = att_strategy[["strategy_code", "transformation_specification"]].to_records(index = False)

        # Resolve prefixes per transformation. We collect any TX that cannot
        # be resolved in a set and report them once at the end, instead of
        # printing one line per occurrence (the previous behaviour produced
        # hundreds of repeated lines when a TX appeared in many strategies).
        missing_txs: set = set()
        strategy_to_txs: Dict[str, List[str]] = {}

        for strategy, transformations in original_strategy_to_txs:
            resolved: List[str] = []
            for transformation in transformations.split("|"):
                prefix = get_tx_prefix(transformation, ssp_txs)
                if prefix is None:
                    missing_txs.add(transformation)
                else:
                    resolved.append(prefix)
            strategy_to_txs[strategy] = resolved

        # Expose the set as an attribute so users can inspect which TXs are
        # not supported by this version of the package.
        self.missing_transformation_codes = sorted(missing_txs)

        if missing_txs:
            msg_lines = [
                "The following transformations are NOT in the "
                "attribute_transformation_code table of the costs_benefits_ssp "
                "package and will therefore be ignored when computing costs "
                "and benefits:",
            ]
            msg_lines.extend(f"  - {tx}" for tx in self.missing_transformation_codes)
            msg_lines.append(
                "If any of these transformations must contribute to the cost "
                "computation, add the corresponding record either via "
                "`insert_cb_records('attribute_transformation_code', ...)` or "
                "by editing the `attribute_transformation_code` sheet of the "
                "Excel configuration file and reloading with "
                "`load_cb_parameters`."
            )
            warnings.warn("\n".join(msg_lines))

        return strategy_to_txs

    def add_additional_columns(
                self,
                ) -> pd.DataFrame:
        
        # Get the data from the ssp outputs
        data = self.ssp_data.copy()

        #add calculation of total TLUs to data
        tlu_conversions = pd.read_sql(self.session.query(LVSTTLUConversion).statement, self.session.bind)


        pop_livestock = data[SSP_GLOBAL_SIMULATION_IDENTIFIERS + [i for i in self.ssp_list_of_vars if "pop_lvst" in i]]
        pop_livestock = pop_livestock.melt(id_vars=['primary_id', 'time_period', 'region', 'strategy_code', 'future_id'])
        pop_livestock = pop_livestock.merge(right=tlu_conversions, on = "variable")

        pop_livestock["total_tlu"] = pop_livestock["value"] * pop_livestock["tlu"]

        pop_livestock_summarized = pop_livestock.groupby(SSP_GLOBAL_SIMULATION_IDENTIFIERS).\
                                                    agg({"total_tlu" : "sum"}).\
                                                    rename(columns={"total_tlu":"lvst_total_tlu"}).\
                                                    reset_index()

        data = data.merge(right = pop_livestock_summarized, on = SSP_GLOBAL_SIMULATION_IDENTIFIERS)

       
        #Calculate the number of people in each sanitation pathway by merging the data with the sanitation classification
        #and with the population data and keepign onyl rows where the population_variable matches the variable.pop
        #then multiply the fraction by the population
        #There was concern that we need to account for differences in ww production between urban and rural
        #But we don't since the pathway fractions for them are mutually exclusive! Hooray!

        sanitation_classification = pd.read_sql(self.session.query(WALISanitationClassificationSP).statement, self.session.bind)

        all_tx_on_ssp_data = list(data.strategy_code.unique())
        all_tx_on_ssp_data.remove(self.strategy_code_base)


        data_strategy = self.cb_get_data_from_wide_to_long(data, all_tx_on_ssp_data, sanitation_classification["variable"].to_list())
        data_strategy = data_strategy.merge(right = sanitation_classification, on='variable')


        population = self.cb_get_data_from_wide_to_long(data, all_tx_on_ssp_data, ['population_gnrl_rural', 'population_gnrl_urban'])
        population = population.rename(columns = {"variable" : "population_variable"})


        data_strategy = data_strategy.merge(right = population[ ["strategy_code", "region", "time_period", "future_id", "population_variable", "value"]], on = ["strategy_code", "region", "time_period", "future_id", "population_variable"], suffixes = ["", ".pop"])
        data_strategy = data_strategy[data_strategy["population_variable"].isin(data_strategy["population_variable"])].reset_index(drop=True)
        data_strategy["pop_in_pathway"] = data_strategy["value"]*data_strategy["value.pop"]


        #Do the same thing with the baseline strategy
        data_base = self.cb_get_data_from_wide_to_long(data, self.strategy_code_base, sanitation_classification["variable"].to_list())
        data_base = data_base.merge(right = sanitation_classification, on = 'variable')


        population_base = self.cb_get_data_from_wide_to_long(data, self.strategy_code_base, ['population_gnrl_rural', 'population_gnrl_urban'])
        population_base = population_base.rename(columns = {"variable" : "population_variable"})

        data_base = data_base.merge(right = population_base[['region', 'time_period', 'future_id', "population_variable", "value"]], on = ['region', 'time_period', 'future_id', "population_variable"], suffixes = ["", ".pop"])

        data_base = data_base[data_base["population_variable"].isin(data_base["population_variable"])].reset_index(drop = True)
        data_base["pop_in_pathway"] = data_base["value"]*data_base["value.pop"]


        data_strategy.merge(right=data_base[['region', 'time_period', 'future_id', 'pop_in_pathway']],  on = ['region', 'time_period', 'future_id'], suffixes = ["", "_base_strat"])



        data_new = pd.concat([data_strategy, data_base], ignore_index = True)

        #reduce it by the sanitation category

        gp_vars = ["primary_id", "region", "time_period", "strategy_code", "future_id", "difference_variable"]

        data_new_summarized = data_new.groupby(gp_vars).agg({"pop_in_pathway" : "sum"}).rename(columns = {"pop_in_pathway" : "value"}).reset_index()
        data_new_summarized = data_new_summarized.rename(columns = {"difference_variable" : "variable"})

        new_list_of_variables = data_new_summarized["variable"].unique()  

        pivot_index_vars = [i for i in data_new_summarized.columns if i not in ["variable", "value"]]
        data_new_summarized_wide = data_new_summarized.pivot(index = pivot_index_vars, columns="variable", values="value").reset_index()  

        data = data.merge(right=data_new_summarized_wide, on = ['primary_id', 'region', 'time_period', 'future_id', 'strategy_code'])

        return data

    ##############################################
    #------ CACHES Y VISTAS PRECOMPUTADAS  ------#
    ##############################################

    def _build_caches(self) -> None:
        """
        Load ONCE into memory every configuration table from SQLite
        (tx_table, cost_factors, transformation_costs, interactions and
        sector-specific inputs) as pandas DataFrames, and build O(1)
        dictionaries that replace the repeated ORM queries.

        Also:
         - precomputes `_diff_var_matches`: a mapping
           {pattern -> [matching ssp columns]} so that `cb_wrapper` does not
           have to scan `ssp_list_of_vars` with a regex on every call.
         - precomputes `_cost_factors_by_name` / `_transformation_costs_by_name`
           / `_tx_table_by_name`: mappings {output_variable_name -> ORM
           instance} that replace `session.query(...).filter(...).first()`
           inside the hot computation path.
        """
        sess = self.session
        bind = sess.bind

        def _df(model):
            return pd.read_sql(sess.query(model).statement, bind)

        self._cache: Dict[str, pd.DataFrame] = {
            "tx_table":                   _df(TXTable),
            "cost_factors":               _df(CostFactor),
            "transformation_costs":       _df(TransformationCost),
            "strategy_interactions":      _df(StrategyInteraction),
            "countries_iso":              _df(CountriesISO),
            "att_dim_time_period":        _df(AttDimTimePeriod),
            "att_transformation_code":    _df(AttTransformationCode),
            "agrc_lvst_productivity_gdp": _df(AgrcLVSTProductivityCostGDP),
            "agrc_rice_mgmt_tx":          _df(AgrcRiceMGMTTX),
            "entc_reduce_losses":         _df(ENTCReduceLosses),
            "ippu_ccs_cost_factor":       _df(IPPUCCSCostFactor),
            "ippu_fgas_designation":      _df(IPPUFgasDesignation),
            "lndu_soil_carbon_fraction":  _df(LNDUSoilCarbonFraction),
            "lvst_enteric_fermentation":  _df(LVSTEntericFermentationTX),
            "lvst_tlu_conversion":        _df(LVSTTLUConversion),
            "pflo_transition_new_diets":  _df(PFLOTransitionNewDiets),
            "wali_sanitation_class":      _df(WALISanitationClassificationSP),
        }

        # Cache ORM records to replace get_cb_var_fields() and the repeated
        # reads inside cb_wrapper. We keep the actual ORM instances so the
        # existing interface is preserved 100% (including the `tx_table`
        # relationship).
        self._tx_table_by_name: Dict[str, TXTable] = {
            r.output_variable_name: r for r in sess.query(TXTable).all()
        }
        self._cost_factors_by_name: Dict[str, CostFactor] = {
            r.output_variable_name: r for r in sess.query(CostFactor).all()
        }
        self._transformation_costs_by_name: Dict[str, TransformationCost] = {
            r.output_variable_name: r for r in sess.query(TransformationCost).all()
        }

        # Precompute the `difference_variable` matches so we do not pay a
        # regex scan over ssp_list_of_vars on every cb_wrapper call.
        self._diff_var_matches: Dict[str, List[str]] = {}

        diff_patterns: List[str] = []
        diff_patterns.extend(
            self._cache["cost_factors"]["difference_variable"].dropna().tolist()
        )
        diff_patterns.extend(
            self._cache["transformation_costs"]["difference_variable"].dropna().tolist()
        )

        for pat in set(diff_patterns):
            if pat is None:
                continue
            # Same treatment as the original code: replace `*` with `.*` and
            # compile as regex. Patterns may also use `|` as an alternative
            # (e.g. several `energy_consumption_trns_*` columns), so we ALWAYS
            # compile — a plain exact lookup is not enough.
            rx = re.compile(pat.replace("*", ".*"))
            self._diff_var_matches[pat] = [
                c for c in self.ssp_list_of_vars if rx.match(c)
            ]

        # Useful precomputed column lists for specific functions.
        self._fgases_vars = [
            v for v in self.ssp_list_of_vars
            if v.startswith("emission_co2e_")
            and not ("_co2_" in v or "_n2o_" in v or "_ch4_" in v or "_subsector_" in v)
        ]
        # Match both the legacy fugitive emissions format
        #   emission_co2e_<gas>_fgtv_fuel_<fuel>
        # and the new stage-split format used by recent SISEPUEDE runs
        #   emission_co2e_<gas>_fgtv_<stage>_fuel_<fuel>
        # where <stage> ∈ {dtp, flaring, venting}. The `.*fuel_` piece is
        # optional in practice because `.*` can match an empty string.
        _fgtv_rx = re.compile(r"emission_co2e_.*_fgtv_.*fuel_.*")
        self._fgtv_vars = [v for v in self.ssp_list_of_vars if _fgtv_rx.match(v)]

        # Derive the set of fuel keys (e.g. "fuel_coal", "fuel_natural_gas")
        # that actually appear in the fgtv columns of this run. This is
        # format-agnostic: for legacy columns `emission_co2e_ch4_fgtv_fuel_coal`
        # and for new columns `emission_co2e_ch4_fgtv_flaring_fuel_coal` the
        # extraction yields the same key `fuel_coal`.
        self._fgtv_fuels = sorted({
            "fuel_" + v.rsplit("fuel_", 1)[-1]
            for v in self._fgtv_vars
        })
        # Only keep fuels that have a matching total energy demand column —
        # these are the ones usable to compute fugitive intensity. In recent
        # runs this typically includes coal, crude, natural_gas and oil; in
        # older runs only coal, natural_gas and oil.
        self._fgtv_energy_vars = [
            f"energy_demand_enfu_total_{fk}"
            for fk in self._fgtv_fuels
            if f"energy_demand_enfu_total_{fk}" in self._ssp_col_set
        ]

        self._ccs_fraction_vars = [
            v for v in self.ssp_list_of_vars
            if v.startswith("frac_ippu_production_with_co2_capture_")
        ]

    def _build_strategy_views(self) -> None:
        """
        Partition `ssp_data` by `strategy_code` once. All `cb_*` functions
        read these views instead of repeatedly calling
        `self.ssp_data.copy()` or `ssp_data[ssp_data.strategy_code==X]`.
        pandas does not guarantee the groupby chunks to be views, but since
        downstream functions never mutate these DataFrames in place (they
        always create new DataFrames through merge/assign), the cached
        reference is safe.
        """
        # Important: `reset_index(drop=True)` so the DataFrames have
        # contiguous indices 0..N-1, just like the original code that did
        # `data[mask].reset_index(drop=True)`. Several downstream functions
        # (e.g. `cb_manure_management_cost`) concatenate along axis 1
        # assuming positional alignment.
        self._ssp_by_strategy: Dict[str, pd.DataFrame] = {
            sc: g.reset_index(drop=True)
            for sc, g in self.ssp_data.groupby("strategy_code", sort=False)
        }

    def _get_ssp_slice(self, strategy_code: str) -> pd.DataFrame:
        """Return the precomputed view for the given strategy."""
        if not hasattr(self, "_ssp_by_strategy"):
            self._build_strategy_views()
        return self._ssp_by_strategy[strategy_code]

    def _invalidate_caches(self) -> None:
        """
        Invalidate caches after the user mutates the configuration database
        (load_cb_parameters, update_all_*_table, etc.). Does not touch
        `_ssp_by_strategy` because ssp_data does not change.
        """
        self._build_caches()

    def insert_cb_records(
                self,
                table_name: str,
                records: Union[pd.DataFrame, List[Dict[str, Any]]],
        ) -> None:
        """
        Insert rows into any configuration table of the package without
        going through the Excel round-trip. Useful for registering new
        transformations, cost factors, interactions, etc. from code.

        Parameters
        ----------
        table_name : str
            Physical table name (the `__tablename__` of the SQLAlchemy
            model). Valid values: `attribute_transformation_code`,
            `tx_table`, `transformation_costs`, `cost_factors`,
            `strategy_interactions`, `countries_by_iso`,
            `attribute_dim_time_period`, `agrc_lvst_productivity_costgdp`,
            `agrc_rice_mgmt_tx`, `entc_reduce_losses_cost_file`,
            `ippu_ccs_cost_factors`, `ippu_fgas_designations`,
            `LNDU_soil_carbon_fractions`, `LVST_enteric_fermentation_tx`,
            `lvst_tlu_conversions`, `pflo_transition_to_new_diets`,
            `wali_sanitation_classification`.
        records : pd.DataFrame | list[dict]
            Rows to add. Columns must match the table schema.

        Notes
        -----
        - Inserts are append-only (existing rows are not replaced). If a
          primary key already exists, SQLAlchemy raises an IntegrityError.
        - After inserting, the internal cache is invalidated so that the
          next `compute_*` calls see the new records.
        """
        if isinstance(records, list):
            df = pd.DataFrame(records)
        elif isinstance(records, pd.DataFrame):
            df = records
        else:
            raise TypeError(
                "records must be pd.DataFrame or list[dict]; "
                f"got {type(records).__name__}"
            )

        if df.empty:
            return

        df.to_sql(
            table_name,
            self.session.bind,
            if_exists="append",
            index=False,
        )
        self._invalidate_caches()

    ##############################################
	#------------- UTILITIES   ------------#
	##############################################


    def tx_in_strategy(
                self,
                tx : str,
                strategy_code : str
        ) -> bool:

        return tx in self.strategy_to_txs[strategy_code]

    #Get a column of data from a wide data table and return it as long for a single strategy
    def cb_get_data_from_wide_to_long(
                self,
                data : pd.DataFrame,
                strategy_code : Union[str, List[str]],
                variables : Union[str, List[str]]
        ) -> pd.DataFrame:

        if not isinstance(variables, list):
            variables = [variables]

        if not isinstance(strategy_code, list):
            strategy_code = [strategy_code]

        # Fast path: if the caller passes `self.ssp_data`, use the
        # precomputed per-strategy views and avoid running a boolean mask
        # over the full DataFrame.
        use_views = (
            data is self.ssp_data
            and hasattr(self, "_ssp_by_strategy")
        )

        cols = SSP_GLOBAL_SIMULATION_IDENTIFIERS + variables

        if use_views:
            if len(strategy_code) == 1:
                data_wide = self._ssp_by_strategy[strategy_code[0]][cols]
            else:
                data_wide = pd.concat(
                    [self._ssp_by_strategy[s][cols] for s in strategy_code],
                    ignore_index=False,
                )
        else:
            data_wide = data[data["strategy_code"].isin(strategy_code)][cols]

        return data_wide.melt(id_vars=SSP_GLOBAL_SIMULATION_IDENTIFIERS)

    def mapping_strategy_specific_functions(
                        self,
                        cb_function : str,
                        cb_orm : Union[TransformationCost, CostFactor],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None
        ) -> Callable:
        
        match cb_function:
            #case 'cb_lndu_soil_carbon':
            #    return self.cb_lndu_soil_carbon(cb_orm)
            case 'cb_difference_between_two_strategies' | 'cb_apply_cost_factors': 
                return self.cb_difference_between_two_strategies(cb_orm)
            case 'cb_scale_variable_in_strategy' : 
                return self.cb_scale_variable_in_strategy(cb_orm)
            case 'cb_fraction_change' : 
                return self.cb_fraction_change(cb_orm)
            case 'cb_entc_reduce_losses' : 
                return self.cb_entc_reduce_losses(cb_orm)
            case 'cb_ippu_clinker' : 
                return self.cb_ippu_clinker(cb_orm)
            case 'cb_ippu_florinated_gases':
                return self.cb_ippu_florinated_gases(cb_orm)
            case 'cb_fgtv_abatement_costs' : 
                return self.cb_fgtv_abatement_costs(cb_orm)
            case 'cb_waso_reduce_consumer_facing_food_waste' : 
                return self.cb_waso_reduce_consumer_facing_food_waste(cb_orm)
            case 'cb_lvst_enteric' : 
                return self.cb_lvst_enteric(cb_orm)
            case 'cb_agrc_rice_mgmt' : 
                return self.cb_agrc_rice_mgmt(cb_orm)
            case 'cb_agrc_lvst_productivity' : 
                return self.cb_agrc_lvst_productivity(cb_orm)
            case 'cb_pflo_healthier_diets' : 
                return self.cb_pflo_healthier_diets(cb_orm)
            case 'cb_ippu_inen_ccs' : 
                return self.cb_ippu_inen_ccs(cb_orm)
            case 'cb_manure_management_cost' : 
                return self.cb_manure_management_cost(cb_orm)

    def get_all_strategies_on_data(
                        self
        ) -> List[str]:
        
        all_strategies = list(self.ssp_data.strategy_code.unique())
        all_strategies.remove(self.strategy_code_base)

        return all_strategies


    ##############################################
	#------ METHODS	   ------#
	##############################################
    
    def get_cb_var_fields(
                        self,
                        cb_var_name : str,
                        ) -> Union[TransformationCost, CostFactor]:

        # Lookup O(1) contra los diccionarios cacheados (reemplaza 2 queries SQL
        # por cada variable por cada estrategia).
        tx_row = self._tx_table_by_name.get(cb_var_name)
        if tx_row is None:
            return None

        if tx_row.cost_type == "system_cost":
            return self._cost_factors_by_name.get(cb_var_name)
        elif tx_row.cost_type == "transformation_cost":
            return self._transformation_costs_by_name.get(cb_var_name)
        return None
        

    def compute_cost_benefit_from_variable(
                        self,
                        cb_var_name : str,
                        strategy_code_tx : str,
                        strategy_code_base : Union[str,None] = None,
                        verbose : bool = True,
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None,
                        cb_var_fields : Union[Dict[str, Union[float,int,str]], None] = None
                        ) -> pd.DataFrame:

        ## Fetch the record from the cache.
        cb_orm = self.get_cb_var_fields(cb_var_name)

        ## Attach the strategy to compare against.
        if strategy_code_tx : cb_orm.strategy_code_tx = strategy_code_tx

        ## Attach the baseline strategy.
        if strategy_code_base :
            cb_orm.strategy_code_base = strategy_code_base
        else:
            cb_orm.strategy_code_base = self.strategy_code_base

        ## Update the record fields if a cb_var_fields dict was received.
        if isinstance(cb_var_fields, dict):
            self.update_cost_factor_register(cb_var_name = cb_var_name, 
                                        cb_var_fields = cb_var_fields)

        if verbose:
            print("---------Costs for: {cb_orm.output_variable_name}.".format(cb_orm=cb_orm))


        if cb_orm.tx_table.cost_type == "system_cost":
            if verbose:
                print("The variable is evaluated in System Cost")
                #print(f"                       {cb_orm.diff_var}")

            
            if cb_orm.cb_var_group == 'wali_sanitation_cost_factors' or cb_orm.cb_var_group == 'wali_benefit_of_sanitation_cost_factors':
                cb_orm.cb_function = 'cb_difference_between_two_strategies'

            # Activate the detailed per-sector × per-fuel fuel cost factors
            # (cb:enfu:fuel_cost:<sector>:<fuel>). These rows live in
            # `cost_factors` with `cb_function='cb_strategy_specific_function'`
            # which is not implemented, so historically they were dead code.
            # Their `difference_variable` values are real SSP output columns
            # (`totalvalue_enfu_fuel_consumed_<sector>_fuel_<fuel>`), so we
            # rescue them to `cb_difference_between_two_strategies`. The
            # aggregate `cb:enfu:fuel_cost:X:X` (cb_var_group='enfu_fuel_cost_factors',
            # note: no `_detail` suffix) is intentionally NOT rescued here to
            # avoid double counting with the detail.
            if cb_orm.cb_var_group == 'enfu_fuel_cost_factors_detail':
                cb_orm.cb_function = 'cb_difference_between_two_strategies'

            if cb_orm.cb_function=="cb:enfu:fuel_cost:X:X":
                cb_orm.cb_function = 'cb_difference_between_two_strategies'

            # Apply the cost function
            if data_baseline and data_tx:
                df_cb_results_var = self.mapping_strategy_specific_functions(cb_orm.cb_function,
                                                                             cb_orm, 
                                                                             data_baseline = data_baseline,
                                                                             data_tx = data_tx)                
            else:    
                df_cb_results_var = self.mapping_strategy_specific_functions(cb_orm.cb_function,cb_orm)
            
            return df_cb_results_var
        elif cb_orm.tx_table.cost_type == "transformation_cost":
            
            print("The variable is evaluated in Transformation Cost")

            if self.tx_in_strategy(cb_orm.transformation_code, cb_orm.strategy_code_tx):

                # Apply the cost function
                if data_baseline and data_tx:
                    df_cb_results_var = self.mapping_strategy_specific_functions(cb_orm.cb_function,
                                                                                 cb_orm,
                                                                                 data_baseline = data_baseline,
                                                                                 data_tx = data_tx)
                else:
                    df_cb_results_var = self.mapping_strategy_specific_functions(cb_orm.cb_function,cb_orm)

                return df_cb_results_var
            else:
                print("The TX is not present in the strategy")
                return pd.DataFrame()

    ######################################################
	#------ METHODOS TO INTERACT WITH THE DATABASE ------#
	######################################################

    def get_all_cost_factor_variables(
                        self
        ) -> pd.DataFrame:

        return self._cache["tx_table"].copy()

    def get_cost_factors(
                        self
        ) -> pd.DataFrame:

        return self._cache["cost_factors"].copy()

    def get_technical_costs(
                        self
        ) -> pd.DataFrame:

        return self._cache["transformation_costs"].copy()
    
    def update_all_cost_factors_table(
                        self,
                        new_cost_factors_table : pd.DataFrame
        ) -> None:

        # Delete all records from cost_factors table
        self.session.query(CostFactor).delete()
        self.session.commit()

        # Bulk insert via SQLAlchemy Core (faster than instantiating one
        # ORM object per row).
        new_cost_factors_table.to_sql(
            "cost_factors", self.session.bind, if_exists="append", index=False
        )

        # Invalidate caches because the records changed.
        self._invalidate_caches()


    def update_all_technical_costs_table(
                        self,
                        new_transformation_costs_table : pd.DataFrame
        ) -> None:

        # Delete all records from transformation_costs table
        self.session.query(TransformationCost).delete()
        self.session.commit()

        # Bulk insert via SQLAlchemy Core.
        new_transformation_costs_table.to_sql(
            "transformation_costs", self.session.bind, if_exists="append", index=False
        )

        # Invalidate caches because the records changed.
        self._invalidate_caches()


    def update_cost_factor_register(self,
                                    cb_var_name : str,
                                    cb_var_fields : Dict[str, Union[float,int,str]]) -> None:
        # Cache lookup instead of SQL query
        tx_row = self._tx_table_by_name.get(cb_var_name)
        if tx_row is None:
            return

        if tx_row.cost_type == "system_cost":
            stmt = update(CostFactor).where(CostFactor.output_variable_name == cb_var_name).values(**cb_var_fields)
            self.session.execute(stmt)
            self.session.commit()
        elif tx_row.cost_type == "transformation_cost":
            stmt = update(TransformationCost).where(TransformationCost.output_variable_name == cb_var_name).values(**cb_var_fields)
            self.session.execute(stmt)
            self.session.commit()

        # Invalidate caches because a record changed.
        self._invalidate_caches()
        


    ##############################################
	#------ SYSTEM COSTS METHODS	   ------#
	##############################################
    
    def compute_system_cost_for_strategy(
                        self,
                        strategy_code_tx : str,
                        strategy_code_base : Union[str,None] = None,
                        verbose : bool = True
                        ) -> pd.DataFrame:
        ## Get cb variables that will be evaluated on system cost (cache lookup)
        tx_df = self._cache["tx_table"]
        system_cost_vars = tx_df.loc[
            tx_df["cost_type"] == "system_cost", "output_variable_name"
        ].tolist()

        accumulate_system_costs = []

        for cb_var_name in system_cost_vars:
            accumulate_system_costs.append(
                self.compute_cost_benefit_from_variable(cb_var_name, strategy_code_tx, verbose=verbose)
            )

        return pd.concat(accumulate_system_costs, ignore_index=True)
    
    def compute_system_cost_for_all_strategies(
                        self,
                        new_system_cost_definition : Union[pd.DataFrame,None] = None,
                        strategy_code_base : Union[str,None] = None,
                        verbose : bool = True
                        ) -> pd.DataFrame:
        
        ## If new_system_cost_definition is not None, update records
        if isinstance(new_system_cost_definition, pd.DataFrame):
            self.update_all_cost_factors_table(new_system_cost_definition)

        all_strategies = self.get_all_strategies_on_data()
        accumulate_system_costs_all_strat = []
        total_strategies = len(all_strategies)

        for id_strat, strategy in enumerate(all_strategies):
            print(f"\n************************************\n*Strategy : {strategy} ({id_strat}/{total_strategies})\n************************************\n")
            accumulate_system_costs_all_strat.append(
                self.compute_system_cost_for_strategy(strategy,verbose = verbose)
            )
        
        return pd.concat(accumulate_system_costs_all_strat, ignore_index = True)

    ##############################################
	#------ TECHNICAL COSTS METHODS	   ------#
	##############################################

    def compute_technical_cost_for_strategy(
                        self,
                        strategy_code_tx : str,
                        strategy_code_base : Union[str,None] = None,
                        verbose : bool = True
                        ) -> pd.DataFrame:
        ## Get cb variables that will be evaluated on technical cost (cache)
        tc_df = self._cache["transformation_costs"]

        cb_tech_cost_mapping_to_tx = dict(
            tc_df[["output_variable_name", "transformation_code"]].to_records(index=False)
        )

        ## Get all transformations in technical cost
        all_tx_in_technical_cost = tc_df["transformation_code"].tolist()

        ## Get transformation inside on strategy_code_tx
        tx_technical_cost_in_strategy = list(set(all_tx_in_technical_cost).intersection(self.strategy_to_txs[strategy_code_tx]))

        if tx_technical_cost_in_strategy:

            accumulate_technical_costs = []

            for cb_var,tx_associated in cb_tech_cost_mapping_to_tx.items():
                if tx_associated in tx_technical_cost_in_strategy:
                    accumulate_technical_costs.append(
                        self.compute_cost_benefit_from_variable(cb_var, strategy_code_tx, verbose = verbose)
                )

            return pd.concat(accumulate_technical_costs, ignore_index = True)
        else:
            print(f"The Strategy {strategy_code_tx} hasn't technical costs")
            return pd.DataFrame()

    def compute_technical_cost_for_all_strategies(
                        self,
                        new_technical_cost_definition : Union[pd.DataFrame,None] = None,
                        strategy_code_base : Union[str,None] = None,
                        verbose : bool = True
                        ) -> pd.DataFrame:
        
        ## If new_technical_cost_definition is not None, update records
        if isinstance(new_technical_cost_definition, pd.DataFrame):
            self.update_all_technical_costs_table(new_technical_cost_definition)

        all_strategies = self.get_all_strategies_on_data()
        accumulate_technical_costs_all_strat = []
        total_strategies = len(all_strategies)

        for id_strat, strategy in enumerate(all_strategies):
            print(f"\n************************************\n*Strategy : {strategy} ({id_strat}/{total_strategies})\n************************************\n")
            accumulate_technical_costs_all_strat.append(
                self.compute_technical_cost_for_strategy(strategy,verbose = verbose)
            )
        
        return pd.concat(accumulate_technical_costs_all_strat, ignore_index = True)

    ##############################################
	#----- METHOD FOR COMPUTING INTERACTIONS ----#
	##############################################

    def cb_process_interactions(
                        self,
                        res : pd.DataFrame,
        ) -> pd.DataFrame:

        # Interaction table from the cache (no SQL on every call).
        interactions = self._cache["strategy_interactions"]
        if interactions.empty:
            return res

        # Precompute the groupby by interaction name once (the previous
        # implementation called `interactions.query("interaction_name==X")`
        # on every iteration — O(S*I) scans over the whole table).
        interaction_groups = {
            name: grp for name, grp in interactions.groupby("interaction_name", sort=False)
        }

        #get the strategies in the results file
        strategies = res["strategy_code"].unique()

        # Collect the subsets to replace in a list instead of reassigning
        # `res = pd.concat([res, ...])` inside the loop, which is quadratic.
        # We perform a single concat at the end.
        rows_to_drop_masks = []
        replacement_frames = []

        for strategy_code in strategies:
            tx_in_strategy = self.strategy_to_txs[strategy_code]

            for interaction, tx_interacting_full in interaction_groups.items():
                # Keep only rows whose tx belongs to the strategy.
                mask_strategy_tx = tx_interacting_full["transformation_code"].isin(tx_in_strategy)
                tx_interacting = tx_interacting_full[mask_strategy_tx]

                if SSP_PRINT_STRATEGIES:
                    print(f"Resolving Interactions in {interaction} : {', '.join(tx_interacting['transformation_code'].to_list())} ")

                if tx_interacting.shape[0] == 0:
                    if SSP_PRINT_STRATEGIES:
                        print(f"No interactions, skipping... {strategy_code}")
                    continue

                # Rescale
                tx_rescale = (tx_interacting.groupby("transformation_code")
                                            .agg({"relative_effect": "mean"})
                                            .reset_index()
                                            .rename(columns={"relative_effect": "original_scalar"}))

                new_sum = tx_rescale["original_scalar"].sum()
                tx_rescale["newscalar"] = tx_rescale["original_scalar"] / new_sum

                tx_interacting = tx_interacting.merge(right=tx_rescale, on="transformation_code")
                tx_interacting = tx_interacting.assign(strategy_code=strategy_code)

                # Subset de `res` a re-escalar.
                vars_of_interest = tx_interacting["variable"].unique()
                mask = (res["strategy_code"] == strategy_code) & (res["variable"].isin(vars_of_interest))
                if not mask.any():
                    continue

                res_subset = res[mask].merge(
                    right=tx_interacting,
                    on=["strategy_code", "variable"],
                    suffixes=("", ".int"),
                )
                res_subset.loc[res_subset["scale_variable"] == 0.0, "newscalar"] = 1.0

                newscalar = res_subset["newscalar"]
                res_subset["value"] = res_subset["value"] * newscalar
                res_subset["difference_value"] = res_subset["difference_value"] * newscalar
                res_subset["variable_value_baseline"] = res_subset["variable_value_baseline"] * newscalar
                res_subset["variable_value_pathway"] = res_subset["variable_value_pathway"] * newscalar

                rows_to_drop_masks.append(mask)
                replacement_frames.append(res_subset[SSP_GLOBAL_COLNAMES_OF_RESULTS])

        if not replacement_frames:
            return res

        # OR-combine every mask and perform ONE drop + ONE concat at the end.
        combined_drop_mask = rows_to_drop_masks[0].copy()
        for m in rows_to_drop_masks[1:]:
            combined_drop_mask |= m

        res_kept = res[~combined_drop_mask]
        return pd.concat([res_kept] + replacement_frames, ignore_index=True)

    ##############################################
	#---------- SHIFT COSTS METHOD  -------------#
	##############################################
    
    def cb_shift_costs(
                        self,
                        res : pd.DataFrame,
        ) -> pd.DataFrame:

        #SHIFT any stray costs incurred from 2015 to 2025 to 2025 and 2035
        # Explicit .copy() to avoid SettingWithCopyWarning: query() returns
        # a view and we assign columns to it right afterwards.
        res_pre2025 = res.query(f"time_period<{SSP_GLOBAL_TIME_PERIOD_TX_START}").copy()
        res_pre2025["variable"] = res_pre2025["variable"] + "_shifted" + (res_pre2025["time_period"]+SSP_GLOBAL_TIME_PERIOD_0).astype(str)#create a new variable so they can be recognized as shifted costs
        res_pre2025["time_period"] = res_pre2025["time_period"]+SSP_GLOBAL_TIME_PERIOD_TX_START #shift the time period

        results_all_pp_shift = pd.concat([res, res_pre2025], ignore_index = True) #paste the results

        results_all_pp_shift.loc[results_all_pp_shift["time_period"]<SSP_GLOBAL_TIME_PERIOD_TX_START,'value'] = 0 #set pre-2025 costs to 0

        return results_all_pp_shift

    ##############################################
	#-- DB EXPORT AND LOAD METHODS   ------------#
	##############################################

    def export_db_to_excel(
                        self,
                        FP_EXCEL_CB_DEFINITION : str = None
        ) -> None:

        list_of_tables = [TXTable,CostFactor,TransformationCost,StrategyInteraction,AgrcLVSTProductivityCostGDP,AgrcRiceMGMTTX,ENTCReduceLosses,
                                                    IPPUCCSCostFactor,IPPUFgasDesignation,LNDUSoilCarbonFraction,
                                                    LVSTEntericFermentationTX,LVSTTLUConversion,PFLOTransitionNewDiets,
                                                    WALISanitationClassificationSP,CountriesISO,AttDimTimePeriod,AttTransformationCode]


        # create a excel writer object
        with pd.ExcelWriter(FP_EXCEL_CB_DEFINITION) as writer:

            for tb in list_of_tables:
                #print(tb.__tablename__)
                df = pd.read_sql(self.session.query(tb).statement, self.session.bind) 
                # use to_excel function and specify the sheet_name and index 
                # to store the dataframe in specified sheet
                df.to_excel(writer, sheet_name=tb.__tablename__, index=False)


    def load_cb_parameters(
        self,
        FP : str
        ) -> None:

        # Test if file exists
        if not os.path.isfile(FP):
            warnings.warn("CB factors file not found\nThe default configuration will be used")
            return

        print("Loading configuration from Excel file (fast path)")

        ## Mapping {sheet_name -> __tablename__ of the SQLAlchemy schema}.
        cb_model_mapping = {
            "tx_table": "tx_table",
            "transformation_costs": "transformation_costs",
            "strategy_interactions": "strategy_interactions",
            "cost_factors": "cost_factors",
            "countries_by_iso": "countries_by_iso",
            "attribute_dim_time_period": "attribute_dim_time_period",
            "attribute_transformation_code": "attribute_transformation_code",
            "agrc_lvst_productivity_costgdp": "agrc_lvst_productivity_costgdp",
            "agrc_rice_mgmt_tx": "agrc_rice_mgmt_tx",
            "entc_reduce_losses_cost_file": "entc_reduce_losses_cost_file",
            "ippu_ccs_cost_factors": "ippu_ccs_cost_factors",
            "ippu_fgas_designations": "ippu_fgas_designations",
            "LNDU_soil_carbon_fractions": "LNDU_soil_carbon_fractions",
            "LVST_enteric_fermentation_tx": "LVST_enteric_fermentation_tx",
            "lvst_tlu_conversions": "lvst_tlu_conversions",
            "pflo_transition_to_new_diets": "pflo_transition_to_new_diets",
            "wali_sanitation_classification": "wali_sanitation_classification",
        }

        ## Open the workbook ONCE (`sheet_name=None` returns a dict with
        ## every sheet already parsed). This avoids opening and parsing
        ## the file 17 times.
        all_sheets = pd.read_excel(FP, engine="openpyxl", sheet_name=None)

        ## New in-memory session + SQLAlchemy schema (no ORM instances).
        engine = create_engine("sqlite:///:memory:")
        Base = declarative_base()
        update_db_schema(Base)
        Base.metadata.create_all(engine)

        ## Bulk load via `to_sql` (SQLAlchemy Core path). Much faster than
        ## instantiating one ORM object per row + bulk_save_objects.
        with engine.begin() as conn:
            for sheet_name, tb_name in cb_model_mapping.items():
                df_tb = all_sheets.get(sheet_name)
                if df_tb is None:
                    warnings.warn(
                        f"Sheet '{sheet_name}' does not exist in {FP}; skipping"
                    )
                    continue
                df_tb.to_sql(tb_name, conn, if_exists="append", index=False)

        Session = sessionmaker(bind=engine)
        self.session = Session()

        # Rebuild caches against the new in-memory session, otherwise the
        # cached ORM instances would be detached.
        self._build_caches()

        print("Database updated")



    #+++++++++++++++++++++++++++++++++++++++++++++    
    ##############################################
	#------ DECORATED METHODS	  ---------------#
    #------ STRATEGY SPECIFIC FUNCTIONS ---------#
    ##############################################
    #+++++++++++++++++++++++++++++++++++++++++++++


    # ---------------------------------------------
    # ------ CB_DIFFERENCE_BETWEEN_TWO_STRATEGIES
    # ---------------------------------------------

    @cb_wrapper
    def cb_difference_between_two_strategies(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost, None] = None,
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None                    
                        ) -> Union[pd.DataFrame, None]:
        
        if cb_orm.cb_function in ['cb_difference_between_two_strategies', 'cb_apply_cost_factors']:
            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                datap_base = data_baseline[SSP_GLOBAL_SIMULATION_IDENTIFIERS + [cb_orm.diff_var]].reset_index(drop = True)
                datap_tx   = data_tx[SSP_GLOBAL_SIMULATION_IDENTIFIERS + [cb_orm.diff_var]].reset_index(drop = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data
                #get the data tables and merge them
                datap_base = data[data["strategy_code"]==cb_orm.strategy_code_base][SSP_GLOBAL_SIMULATION_IDENTIFIERS + [cb_orm.diff_var]].reset_index(drop = True)
                datap_tx   = data[data["strategy_code"]==cb_orm.strategy_code_tx][SSP_GLOBAL_SIMULATION_IDENTIFIERS + [cb_orm.diff_var]].reset_index(drop = True)
                
            datap_base = datap_base.drop(columns=["primary_id", "strategy_code"])

            tx_suffix = '_tx'
            base_suffix = '_base'

            data_merged = datap_tx.merge(right = datap_base, on =  ['region', 'time_period', 'future_id'], suffixes=(tx_suffix, base_suffix))

            #Calculate the difference in variables and then apply the multiplier, which may change over time
            #Assume cost change only begins in 2023

            data_merged["difference_variable"] = cb_orm.diff_var

            data_merged["difference_value"] = data_merged[f"{cb_orm.diff_var}{tx_suffix}"] - data_merged[f"{cb_orm.diff_var}{base_suffix}"]

            data_merged["time_period_for_multiplier_change"] = np.maximum(0, data_merged["time_period"] - SSP_GLOBAL_TIME_PERIOD_2023)

            data_merged["variable"] = cb_orm.output_variable_name

            data_merged["value"] = data_merged["difference_value"]*cb_orm.multiplier*cb_orm.annual_change**data_merged["time_period_for_multiplier_change"]

            ## Used to compute the difference between the baseline strategy and the pathway.
            data_merged['variable_value_baseline'] =  data_merged[f"{cb_orm.diff_var}{base_suffix}"]
            data_merged['variable_value_pathway'] = data_merged[f"{cb_orm.diff_var}{tx_suffix}"]

            data_merged = data_merged[SSP_GLOBAL_COLNAMES_OF_RESULTS]
            
            return data_merged
        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_difference_between_two_strategies function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None

    # ---------------------------------------------
    # ------ CB_SCALE_VARIABLE_IN_STRATEGY 
    # ---------------------------------------------

    #This function calculates costs and benefits as just a scalar applied to a variable within
    # a single strategy. It uses code from cb_difference_between_two_strategies, so the use of
    # data_merged, for example, is holdover from that function.
    @cb_wrapper
    def cb_scale_variable_in_strategy(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None
                        ) -> pd.DataFrame:


        if cb_orm.cb_function == 'cb_scale_variable_in_strategy':
            
            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data

            id_vars  = ['region','time_period', 'strategy_code', 'future_id']
            
            datap_tx = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, cb_orm.diff_var)

            #This is code copied over from another function, so data_merged is just datap_tx
            data_merged = datap_tx.copy()
            data_merged["difference"] = data_merged["value"]
            
            data_merged["time_period_for_multiplier_change"] = np.maximum(0,data_merged["time_period"]-SSP_GLOBAL_TIME_PERIOD_2023)
            data_merged["values"] = data_merged["difference"]*cb_orm.multiplier*cb_orm.annual_change**data_merged["time_period_for_multiplier_change"]  

            tmp = data_merged[id_vars].copy()
            tmp["difference_variable"] = cb_orm.diff_var
            tmp["difference_value"] = data_merged["difference"]
            tmp["variable"] = cb_orm.output_variable_name
            tmp["value"] = data_merged["values"]

            ## Used to compute the difference between the baseline strategy and the pathway.
            ## In this case the baseline value is set to zero.
            tmp['variable_value_baseline'] = 0
            if "difference" in list(tmp.columns):
                tmp['variable_value_pathway'] = tmp["difference_value"]
            else:
                tmp['variable_value_pathway'] = 0.0
            output = tmp.copy()
        
            return output 
        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_scale_variable_in_strategy function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None

    # ---------------------------------------------
    # ------ CB_FRACTION_CHANGE 
    # ---------------------------------------------
    #This function calculates the costs adn benefits as a multiplier applied to the
    #difference in a variable between two strategies, where that difference is
    #defined by some change in a factor, e.g., km/l or tons-waste/person. frac_var
    #gives the name of the variable that has the fraction. Invert tells us whether we
    #need to flip the fractions ot make our calculation correct. If our effect variable
    #is already in the denominator of our fraction (e.g., effect in L, fraction is km/L) then
    #we are good. If the effect variable is in the numerator (e.g., effect in T, fraction is 
    #T/person) then we need to flip it.
    #To be specific, let E_tx be the effect we observe (e.g., L of fuel) in the transformation
    #let f_base and f_tx be the fractions of km/L in the base and transformed futures
    #then the distance that has been traveled in the transformation, d_tx = E_tx*f_tx. 
    #traveling that same distance with the old efficiency would have required 
    #E_base = d_tx/f_base L. So, E_tx-E_base = E_tx - d_tx/f_base = E_tx - Etx*f_tx/f_base
    # = E_tx(1-ftx/f_base)
    @cb_wrapper
    def cb_fraction_change(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None        
                        ) -> pd.DataFrame:

        if cb_orm.cb_function == "cb_fraction_change":

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data
            
            invert = cb_orm.arg2
            frac_var = cb_orm.arg1

            #get tech change in fractions
            fraction_tx = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, frac_var)
            fraction_base = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_base, frac_var)

            if invert == 1 :
                fraction_tx["value"] = 1/fraction_tx["value"]
                fraction_base["value"] = 1/fraction_base["value"]
            
            data_merged = fraction_tx.merge(right = fraction_base, on = ['time_period', 'region', 'variable'], suffixes = ['.tx_frac', '.ba_frac'])
            data_merged["fraction_change"] = data_merged["value.tx_frac"]/data_merged["value.ba_frac"]
            data_merged["fraction_multiplier"] = (1-data_merged["fraction_change"])
        

            #get the output results
            output_tx = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, cb_orm.diff_var)
            data_merged = data_merged.merge(right = output_tx, on = ['time_period', 'region'], suffixes=['.tx_frac', '.effect'])
            data_merged = data_merged.rename(columns = {"value" : "effect_value"})
        
            #get the avoided value
            data_merged["difference_variable"] = cb_orm.diff_var
            data_merged["difference_value"] = data_merged["effect_value"]*data_merged["fraction_multiplier"]
            data_merged["variable"] = cb_orm.output_variable_name

            data_merged["time_period_for_multiplier_change"] = np.maximum(0,data_merged["time_period"]-SSP_GLOBAL_TIME_PERIOD_2023)
            data_merged["value"] = data_merged["difference_value"]*cb_orm.multiplier*cb_orm.annual_change**data_merged["time_period_for_multiplier_change"]    
        
            GUARDA_COLS = list(set(data_merged.columns).intersection(SSP_GLOBAL_COLNAMES_OF_RESULTS))
            data_merged_results = data_merged[GUARDA_COLS]
        
            #any divide-by-zero NAs from our earlier division gets a 0
            data_merged_results = data_merged_results.replace(np.nan, 0.0)
            
            ## Used to compute the difference between the baseline strategy and the pathway.
            ## In this case the baseline value is set to zero.
            data_merged_results['variable_value_baseline'] = 0 
            data_merged_results['variable_value_pathway'] = data_merged["difference_value"]

            return data_merged_results

        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_fraction_change function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None

    #----------ENTC:REDUCE_LOSSES: Technical cost of maintaining grid ----------
    @cb_wrapper
    def cb_entc_reduce_losses(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None                        
                        ) -> pd.DataFrame:

        if cb_orm.cb_function == "cb_entc_reduce_losses":

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data

            # Cache lookups (sin SQL por llamada)
            cb_transmission_loss_costs = self._cache["entc_reduce_losses"].merge(
                right=self._cache["countries_iso"], on="iso_code3"
            )
        
            data_strategy = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, cb_orm.diff_var)

            data_output = data_strategy.merge(right = cb_transmission_loss_costs, on = 'region')

            data_output["variable"] = cb_orm.output_variable_name
            data_output["value"] = data_output["annual_investment_USD"]
            data_output["difference_variable"] = 'N/A (constant annual cost)'
            data_output["difference_value"] = data_output["annual_investment_USD"]

            ## Used to compute the difference between the baseline strategy and the pathway.
            ## In this case both baseline and pathway values are set to zero.
            data_output['variable_value_baseline'] = 0 
            data_output['variable_value_pathway'] = 0

            data_output = data_output[SSP_GLOBAL_COLNAMES_OF_RESULTS] 
        
            return data_output
        
        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_entc_reduce_losses function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None

    #----------IPPU:CLINKER------------------}
    @cb_wrapper
    def cb_ippu_clinker(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None                        
                        ) -> pd.DataFrame:

        if cb_orm.cb_function == "cb_ippu_clinker":

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)                
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data
                #get the data tables and merge them
                data_baseline = data[data["strategy_code"]==cb_orm.strategy_code_base][SSP_GLOBAL_SIMULATION_IDENTIFIERS + [cb_orm.diff_var]].reset_index(drop = True)
                data_tx   = data[data["strategy_code"]==cb_orm.strategy_code_tx][SSP_GLOBAL_SIMULATION_IDENTIFIERS + [cb_orm.diff_var]].reset_index(drop = True)
                
            #get the clinker fraction data
            #ORIGINALMENTE USABAMOS ESTA FUNCION PARA CONSTRUIR EL DATAFRAME. DEBEMOS VERIFICAR ESTO
            #diff_clinker = self.mapping_strategy_specific_functions("cb_difference_between_two_strategies",cb_orm)
            datap_base = data_baseline[SSP_GLOBAL_SIMULATION_IDENTIFIERS + [cb_orm.diff_var]].reset_index(drop = True)
            datap_tx   = data_tx[SSP_GLOBAL_SIMULATION_IDENTIFIERS + [cb_orm.diff_var]].reset_index(drop = True)

            datap_base = datap_base.drop(columns=["primary_id", "strategy_code"])

            tx_suffix = '_tx'
            base_suffix = '_base'

            diff_clinker = datap_tx.merge(right = datap_base, on =  ['region', 'time_period', 'future_id'], suffixes=(tx_suffix, base_suffix))

            #Calculate the difference in variables and then apply the multiplier, which may change over time
            #Assume cost change only begins in 2023

            diff_clinker["difference_variable"] = cb_orm.diff_var

            diff_clinker["difference_value"] = diff_clinker[f"{cb_orm.diff_var}{tx_suffix}"] - diff_clinker[f"{cb_orm.diff_var}{base_suffix}"]

            diff_clinker["time_period_for_multiplier_change"] = np.maximum(0, diff_clinker["time_period"] - SSP_GLOBAL_TIME_PERIOD_2023)

            diff_clinker["variable"] = cb_orm.output_variable_name

            diff_clinker["value"] = diff_clinker["difference_value"]*cb_orm.multiplier*cb_orm.annual_change**diff_clinker["time_period_for_multiplier_change"]

            GUARDA_COLS = list(set(diff_clinker.columns).intersection(SSP_GLOBAL_COLNAMES_OF_RESULTS))

            diff_clinker = diff_clinker[GUARDA_COLS]

            data_amt_cement = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, 'prod_ippu_cement_tonne')
            
            data_merged = diff_clinker.merge(right = data_amt_cement, on = ['region', 'time_period'], suffixes = ["", ".cement"])
            
            data_merged["difference_value"] = data_merged["value.cement"]/(1-data_merged["difference_value"]) - data_merged["value.cement"]
            data_merged["value"] = data_merged["difference_value"]*cb_orm.multiplier
            
                
            data_output = data_merged[diff_clinker.columns.to_list()].copy()
            
            ## Used to compute the difference between the baseline strategy and the pathway.
            ## In this case the baseline value is set to zero.
            data_output['variable_value_baseline'] = 0 

            if "difference_value"in list(data_merged.columns):
                data_output['variable_value_pathway'] = data_merged["difference_value"]
            else:
                data_output['variable_value_pathway'] = 0.0
            return data_output

        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_ippu_clinker function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None

    #----------IPPU:FGASES-------------------
    @cb_wrapper
    def cb_ippu_florinated_gases( 
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None                        
                        ) -> pd.DataFrame:

        if cb_orm.cb_function == "cb_ippu_florinated_gases":

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data

            #get all the variables with florinated gases (precomputado en cache)
            fgases = self._fgases_vars

            #sum up for both strategies
            data_strategy = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, fgases)
        
            data_strategy_summarized = data_strategy.groupby(["region", "time_period", "strategy_code"]).agg({"value" : "sum"}).rename(columns = {"value" : "difference_value"}).reset_index()
            data_strategy_summarized["difference_variable"] = 'emission_co2e_all_fgases_ippu'
            # Fix: the old code referenced an undefined name `output_vars`.
            # The intended value is the configured output variable name,
            # matching what the baseline branch assigns a few lines below.
            data_strategy_summarized["variable"] = cb_orm.output_variable_name


            data_strategy_base = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_base, fgases)
            data_strategy_base_summarized = data_strategy_base.groupby(["region", "time_period", "strategy_code"]).agg({"value" : "sum"}).rename(columns = {"value" : "difference_value"}).reset_index()
            data_strategy_base_summarized["difference_variable"] = 'emission_co2e_all_fgases_ippu'
            data_strategy_base_summarized["variable"] = cb_orm.output_variable_name


            #take difference and multiply by cost / CO2e
            data_fgases_merged = data_strategy_summarized.merge(right = data_strategy_base_summarized, on = ['region', 'time_period'], suffixes = ["", ".base"])
            data_fgases_merged["difference_value"] = data_fgases_merged["difference_value"] - data_fgases_merged["difference_value.base"]
            data_fgases_merged["value"] = data_fgases_merged["difference_value"]*cb_orm.multiplier
        
            data_fgases_merged = data_fgases_merged[["region","time_period","strategy_code", "difference_variable", "difference_value","variable","value"]]

            ## Used to compute the difference between the baseline strategy and the pathway.
            data_fgases_merged['variable_value_baseline'] = data_fgases_merged["difference_value.base"]
            data_fgases_merged['variable_value_pathway'] = data_fgases_merged["difference_value"]

            #return result
            return data_fgases_merged
        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_ippu_florinated_gases function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None


    #--------------FGTV: ALL COSTS ------------
    #This function calculates the cost of abating fugitive emissions.
    #(1) calculates the "fugitive emissions intensity" as the fugitive emissions per unit of energy consumed in each time period in the baseline
    #(2) calculates the "expected fugitive emissions" in a transformed future by multiplying that intensity by the energy consumed in the transformed future
    #(3) calculates "fugitive emissions abated"  as difference between "expected fugitive emissions" and actual emissions
    #(4) calculates "cost of abatement" as quantity abated * cost of abatement
    @cb_wrapper
    def cb_fgtv_abatement_costs(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None                        
                        ) -> pd.DataFrame:
        
        if cb_orm.cb_function == "cb_fgtv_abatement_costs":

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data
                    
            #(1) FUGITIVE EMISSIONS INTENSITY
            # Both `energy_vars` and `fgtv_vars` are precomputed in
            # `_build_caches` and are format-agnostic: they work both for the
            # legacy fugitive columns (emission_co2e_<gas>_fgtv_fuel_<fuel>)
            # and for the new stage-split format
            # (emission_co2e_<gas>_fgtv_<stage>_fuel_<fuel>). The fuel list is
            # derived from the actual columns present in this run, so adding
            # `crude` as a new fuel in recent SISEPUEDE versions is handled
            # automatically.
            energy_vars = self._fgtv_energy_vars
            fgtv_vars = self._fgtv_vars

            if not fgtv_vars or not energy_vars:
                # This run has no fugitive-emission columns at all, so there
                # is nothing to compute. Returning an empty DataFrame (with
                # the expected columns) lets the caller concat it cleanly.
                return pd.DataFrame(columns=SSP_GLOBAL_COLNAMES_OF_RESULTS)

            #1. Get the fugitive emissions per PJ of coal and oil together in the baseline
            # Vectorized string ops. The `rsplit("fuel_", n=1)` pattern extracts
            # the fuel key regardless of whether the variable is legacy
            # (`..._fgtv_fuel_coal`) or split (`..._fgtv_flaring_fuel_coal`).
            energy = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_base, energy_vars)
            energy["fuel"] = energy["variable"].str.replace("energy_demand_enfu_total_", "", regex=False)
            fgtv = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_base, fgtv_vars)
            fgtv["fuel"] = "fuel_" + fgtv["variable"].str.rsplit("fuel_", n=1).str[-1]

            #1.a summarize the emissions by fuel
            vars_to_groupby = ["primary_id", "region", "time_period", "strategy_code", "fuel"]
            fgtv = fgtv.groupby(vars_to_groupby).agg({"value" : "sum"}).reset_index()

            data_merged_base = energy.merge(right = fgtv, on = vars_to_groupby, suffixes=('.en_base', '.fg_base'))

            #2. Get the fugitive emissions per PJ of coal and oil together in the transformed future
            energy_tx = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, energy_vars)
            energy_tx["fuel"] = energy_tx["variable"].str.replace("energy_demand_enfu_total_", "", regex=False)
            fgtv_tx = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, fgtv_vars)
            fgtv_tx["fuel"] = "fuel_" + fgtv_tx["variable"].str.rsplit("fuel_", n=1).str[-1]

            #2.b summarize the emissions by fuel
            fgtv_tx = fgtv_tx.groupby(vars_to_groupby).agg({"value" : "sum"}).reset_index()

            data_merged_tx = energy_tx.merge(right = fgtv_tx, on = vars_to_groupby, suffixes=('.en_tx', '.fg_tx'))

            #3. Merge the two together
            data_merged = data_merged_tx.merge(right = data_merged_base, on = ['region', 'time_period', 'fuel'], suffixes=['.tx', '.base'])
            #(2/3) FUGITIVE EMISSIONS INTENSITY and EXPECTED FUGITIVE EMISSIONS
            #4. Calculate the fugitive emissions per unit demand in the baseline and apply it to the transformed future
            data_merged["fgtv_co2e_per_demand_base"] = data_merged["value.fg_base"]/data_merged["value.en_base"]
            data_merged["fgtv_co2e_expected_per_demand"] = data_merged["value.en_tx"]*data_merged["fgtv_co2e_per_demand_base"]
        
            #5. Calculate the difference between observed and expected demand
            data_merged["difference_value"] = data_merged["value.fg_tx"] - data_merged["fgtv_co2e_expected_per_demand"]
            data_merged["difference_variable"] = data_merged["variable.tx"]

            #6. Apply the multiplier
            data_merged["value"] = data_merged["difference_value"]*cb_orm.multiplier
            data_merged["variable"] = cb_orm.output_variable_name

            #7. Get columns
            data_merged["strategy_code"] = data_merged["strategy_code.tx"]
            data_merged["future_id"]= data_merged["future_id.tx"]

            ## Used to compute the difference between the baseline strategy and the pathway.
            data_merged['variable_value_baseline'] = data_merged["fgtv_co2e_expected_per_demand"]
            data_merged['variable_value_pathway'] = data_merged["value.fg_tx"] 


            data_merged = data_merged[SSP_GLOBAL_COLNAMES_OF_RESULTS]

            #8. If there are NaNs/NAs in the value, replace them with 0.
            # Fix: the previous code called `.replace(np.nan, 0.0)` without
            # assigning the result, so the replacement was silently dropped.
            # This is especially important for fuels like `crude` whose
            # baseline demand may be zero, producing 0/0 = NaN in the
            # intensity calculation.
            data_merged = data_merged.fillna(0.0)

            return data_merged
        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_fgtv_abatement_costs function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None
        
    #----------WASO:WASTE REDUCTION TECHNICAL COSTS------------------

    # This function calculates consumer food waste avoided, which includes everythign after
    #the retailer. From james:
    #  consumer_food_waste_avoided = (qty_waso_total_food_produced_tonne - 
    #qty_agrc_food_produced_lost_sent_to_msw_tonne) * 
    #  (1 - factor_waso_waste_per_capita_scalar_food)/factor_waso_waste_per_capita_scalar_food
    @cb_wrapper
    def cb_waso_reduce_consumer_facing_food_waste(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None                        
                        ) -> pd.DataFrame:
        
        if cb_orm.cb_function == "cb_waso_reduce_consumer_facing_food_waste":

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data

        
            cols_required = ['qty_waso_total_food_produced_tonne', 'qty_agrc_food_produced_lost_sent_to_msw_tonne','factor_waso_waste_per_capita_scalar_food']

            food_waste_data = data[data["strategy_code"]==cb_orm.strategy_code_tx][SSP_GLOBAL_SIMULATION_IDENTIFIERS + cols_required].reset_index(drop = True).copy()
        
            #Get teh consumer food waste amount
            food_waste_data["consumer_food_waste"] = (food_waste_data["qty_waso_total_food_produced_tonne"]) 
                                                #KLUDGE 07.06/2023
            #UNCOMMENT THIS LINE WHEN JAMES FIXES WHAT 'qty_waso_total_food_produced_tonne' means
                                                #Because of a bug, this is already consumer food waste.
                                            # - food_waste_data$qty_agrc_food_produced_lost_sent_to_msw_tonne)

            #Get how much would have been there
            food_waste_data["consumer_food_waste_counterfactual"] = food_waste_data["consumer_food_waste"]/food_waste_data["factor_waso_waste_per_capita_scalar_food"]


            #get the difference, whic his hte avoided amount
            food_waste_data["consumer_food_waste_avoided"] = food_waste_data["consumer_food_waste"] - food_waste_data["consumer_food_waste_counterfactual"]

            food_waste_data["consumer_food_waste_avoided2"] = (food_waste_data["qty_waso_total_food_produced_tonne"] - food_waste_data["qty_agrc_food_produced_lost_sent_to_msw_tonne"]) *(1-food_waste_data["factor_waso_waste_per_capita_scalar_food"])/ food_waste_data["factor_waso_waste_per_capita_scalar_food"]
        
            food_waste_to_merge = food_waste_data[SSP_GLOBAL_SIMULATION_IDENTIFIERS + ['consumer_food_waste_avoided']]

            outputs = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, 'qty_waso_total_food_produced_tonne')

            merged_data = outputs.merge(right = food_waste_to_merge, on=['strategy_code', 'region', 'time_period'], suffixes=['', '.food'])

            
            merged_data["difference_variable"] = 'qty_consumer_food_waste_avoided'
            merged_data["difference_value"] = merged_data["consumer_food_waste_avoided"]
            merged_data["variable"] = cb_orm.output_variable_name
            merged_data["value"] = merged_data["difference_value"] * cb_orm.multiplier

            ## Used to compute the difference between the baseline strategy and the pathway.
            merged_data['variable_value_baseline'] = food_waste_data["consumer_food_waste_counterfactual"]
            merged_data['variable_value_pathway'] = food_waste_data["consumer_food_waste"]

            merged_data = merged_data[SSP_GLOBAL_COLNAMES_OF_RESULTS]

            return merged_data
        
        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_waso_reduce_consumer_facing_food_waste function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None
        
    #----------LVST: ENTERIC FERMENTATION------------------
    @cb_wrapper
    def cb_lvst_enteric(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None                        
                        ) -> pd.DataFrame:
        
        if cb_orm.cb_function == "cb_lvst_enteric":

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data

            #define the strategy as the fractino of livestock receivving this intervention in a particular year
            tx_definition = self._cache["lvst_enteric_fermentation"]
            affected_livestock = tx_definition[tx_definition["application"]>0]
            timesteps = pd.DataFrame({"time_period" : range(SSP_GLOBAL_TIME_PERIODS)})

            enteric_pop_fracs = affected_livestock.merge(right=timesteps, how = "cross")

            enteric_pop_fracs["application_in_year"] = enteric_pop_fracs["application"]/(SSP_GLOBAL_TIME_PERIODS - SSP_GLOBAL_TIME_PERIOD_TX_START) *(enteric_pop_fracs["time_period"] - SSP_GLOBAL_TIME_PERIOD_TX_START+1)
            enteric_pop_fracs.loc[(enteric_pop_fracs["time_period"] >=0) & (enteric_pop_fracs["time_period"]<=SSP_GLOBAL_TIME_PERIOD_TX_START-1), "application_in_year"] = 0


            #apply that to the data
            data_num_livestock = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, affected_livestock["variable"].to_list())
        
            data_merged = data_num_livestock.merge(right = enteric_pop_fracs, on = ['variable', 'time_period'])
            data_merged["difference_variable"] = data_merged["variable"] 
            data_merged["difference_value"] = data_merged["value"]*data_merged["application_in_year"]
            #data_merged["variable"] = data_merged["variable"].apply(lambda x : f"{cb_orm.output_variable_name[:-1]}{x}")
            data_merged["variable"] = cb_orm.output_variable_name
            data_merged["value"] = data_merged["difference_value"]*cb_orm.multiplier

            GUARDA_COLS = list(set(data_merged.columns).intersection(SSP_GLOBAL_COLNAMES_OF_RESULTS))
            
            #data_merged = data_merged[SSP_GLOBAL_COLNAMES_OF_RESULTS]
            data_merged = data_merged[GUARDA_COLS]

            ## Used to compute the difference between the baseline strategy and the pathway.
            ## In this case the baseline value is set to zero.
            data_merged['variable_value_baseline'] = 0 

            if "difference_value" in list(data_merged.columns):
                data_merged['variable_value_pathway'] = data_merged["difference_value"]
            else:
                data_merged['variable_value_pathway'] = 0.0
            return data_merged 

        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_waso_reduce_consumer_facing_food_waste function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None

    #----------AGRC:RICE------------
    @cb_wrapper
    def cb_agrc_rice_mgmt(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None                        
                        ) -> pd.DataFrame:
        
        if cb_orm.cb_function == "cb_agrc_rice_mgmt":

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data
        
            #define the transformation as the fraction of acres receiivng better rice management
            # Lightweight copy of the cached frame because we are about to
            # add the `level_of_implementation` column and we do not want to
            # contaminate the cached version.
            tx_definition = self._cache["agrc_rice_mgmt_tx"].copy()
            tx_definition["level_of_implementation"] = (1-tx_definition["ef_agrc_anaerobicdom_rice_kg_ch4_ha"])/0.45
            
            rice_management_data = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, cb_orm.diff_var)

            #merge with transformation
            rice_management_data = rice_management_data.merge(right = tx_definition, on = 'time_period')
        
            rice_management_data["difference_variable"] = cb_orm.diff_var #paste0('diff_', diff_var)
            rice_management_data["difference_value"] = rice_management_data["value"]*rice_management_data["level_of_implementation"]
            rice_management_data["variable"] = cb_orm.output_variable_name
            rice_management_data["value"] = rice_management_data["difference_value"]*cb_orm.multiplier

            ## Used to compute the difference between the baseline strategy and the pathway.
            ## In this case the baseline value is set to zero.
            rice_management_data['variable_value_baseline'] = 0 
            rice_management_data['variable_value_pathway'] = rice_management_data["difference_value"]

            rice_management_data = rice_management_data[SSP_GLOBAL_COLNAMES_OF_RESULTS]
            
            return rice_management_data

        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_agrc_rice_mgmt function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None

    #----------AGRCLVST:Productivity----------
    #the economic cost of increasing productivity is equal to
    #some percent of GDP defined in file
    @cb_wrapper
    def cb_agrc_lvst_productivity(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None                        
                        ) -> pd.DataFrame:
        
        if cb_orm.cb_function == "cb_agrc_lvst_productivity":

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data

            #Get the gdp data
            gdp = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, 'gdp_mmm_usd')

            #Get the fractions for each country (cache lookup)
            gdp_fracs = self._cache["agrc_lvst_productivity_gdp"].rename(
                columns={"cost_of_productivity_improvements_pct_gdp": "cost_frac"}
            )

            #country codes (cache lookup)
            country_codes = self._cache["countries_iso"]

            gdp_fracs = gdp_fracs.merge(right = country_codes, on = "iso_code3")
            gdp_fracs = gdp_fracs.rename(columns = {"REGION" : "region"})
            gdp_fracs = gdp_fracs[['region', 'cost_frac']]

            #merge wiht gdp
            gdp = gdp.merge(right=gdp_fracs, on = "region")
            gdp["difference_variable"] = 'diff_fraction_of_GDP_for_productivity'
            gdp["difference_value"] = gdp["cost_frac"]
            gdp["variable"] = cb_orm.output_variable_name
            gdp["value"] = gdp["value"]*10**9*gdp["cost_frac"]/2*(-1)

            escalar_prod = 1

            if cb_orm.strategy_code_tx == 'PFLO:UNCONSTRAINED':
                escalar_prod = 1.2
            
            gdp["value"] *= escalar_prod

            gdp.loc[gdp["time_period"]<SSP_GLOBAL_TIME_PERIOD_TX_START, "value"] = 0

            ## Used to compute the difference between the baseline strategy and the pathway.
            ## In this case both baseline and pathway values are set to zero.
            gdp['variable_value_baseline'] = 0
            gdp['variable_value_pathway'] = 0
            gdp = gdp[SSP_GLOBAL_COLNAMES_OF_RESULTS]

            return gdp

        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_agrc_lvst_productivity function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None

    #--------------PFLO:BETTER DIETS------------
    #calculate the number of additional people using better diets
    #for each such person, there is a $370 cost savings in groceries and 
    #$1000/yr cost savings in health
    @cb_wrapper
    def cb_pflo_healthier_diets(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None                        
                        ) -> pd.DataFrame:
        
        if cb_orm.cb_function == "cb_pflo_healthier_diets":

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data

            #Get the population
            population = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, ['population_gnrl_rural', 'population_gnrl_urban'])
            
            vars_to_groupby = ["primary_id", "region", "time_period", "strategy_code", "future_id"]

            total_pop = population.groupby(vars_to_groupby).agg({"value" : "sum"}).rename(columns = {"value" : "total_pop"}).reset_index()

            #get the file with popualtion fractions (cache lookup)
            diet_frac = self._cache["pflo_transition_new_diets"]
            
            data_merged = total_pop.merge(right = diet_frac, on='time_period')
            data_merged["difference_value"] = data_merged["total_pop"]*(1-data_merged["frac_gnrl_w_original_diet"])
            data_merged["difference_variable"] = 'pop_with_better_diet'
            data_merged["variable"] = cb_orm.output_variable_name
            data_merged["value"] = data_merged["difference_value"]*cb_orm.multiplier

            ## Used to compute the difference between the baseline strategy and the pathway.
            ## In this case the baseline value is set to zero.
            data_merged['variable_value_baseline'] = 0 
            data_merged['variable_value_pathway'] = data_merged["difference_value"]

            data_merged = data_merged[SSP_GLOBAL_COLNAMES_OF_RESULTS]

            return data_merged

        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_pflo_healthier_diets function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None

    #--------------IPPU: CCS ------------------
    @cb_wrapper
    def cb_ippu_inen_ccs(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None                        
                        ) -> pd.DataFrame:
        
        if cb_orm.cb_function == "cb_ippu_inen_ccs":

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data
        
            #get the fraction reductions in CO2 (usa la lista precomputada)
            ccs_fraction_vars = self._ccs_fraction_vars
            ccs_fractions = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, ccs_fraction_vars)

            #given the global capture rate, update the application fraction
            ccs_fractions["application_rate"] = ccs_fractions["value"]

            #get the quantities of production for those variables
            production_vars = [i.replace("frac_ippu_production_with_co2_capture_", "prod_ippu_") + "_tonne" for i in ccs_fraction_vars]
            # Vectorized str ops en lugar de .apply(lambda).
            ccs_fractions["variable"] = (
                ccs_fractions["variable"]
                .str.replace("frac_ippu_production_with_co2_capture_", "prod_ippu_", regex=False)
                + "_tonne"
            )
            prod_qty = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, production_vars)

            #merge the two datasets
            by_merge_vars = ['region', 'strategy_code', 'time_period', 'variable']
            tx_suffix = "ccs"
            base_suffix = ""

            data_merged = ccs_fractions.merge(right = prod_qty, on =  by_merge_vars, suffixes=(tx_suffix, base_suffix))

            #multiply the production quantity by the fractions
            data_merged["difference_value"] = data_merged["application_rate"] * data_merged["value"]
            data_merged["difference_variable"] = data_merged["variable"]

            #read the cost definitions (cache lookup)
            ccs_cost_factor = self._cache["ippu_ccs_cost_factor"]
            
            data_merged = data_merged.merge(right = ccs_cost_factor, on = 'variable')

            data_merged["value"] = data_merged["difference_value"] * data_merged["multiplier"]
            data_merged["variable"] = data_merged["output_variable_name"]

            ## Used to compute the difference between the baseline strategy and the pathway.
            ## In this case both baseline and pathway values are set to zero.
            data_merged['variable_value_baseline'] = 0 
            data_merged['variable_value_pathway'] = 0

            data_merged = data_merged[SSP_GLOBAL_COLNAMES_OF_RESULTS]
            
            return data_merged

        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_ippu_inen_ccs function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None

    #---------------Manure Management
    @cb_wrapper
    def cb_manure_management_cost(
                        self,
                        cb_orm : Union[CostFactor,TransformationCost],
                        data_baseline : Union[pd.DataFrame, None] = None,
                        data_tx :  Union[pd.DataFrame, None] = None                        
                        ) -> pd.DataFrame:
        
        if cb_orm.cb_function == "cb_manure_management_cost":

            if isinstance(data_baseline, pd.DataFrame) and isinstance(data_tx, pd.DataFrame):
                # Get the data from the ssp outputs
                data = pd.concat([data_baseline, data_tx], ignore_index = True)
            else:
                # Get the data from the ssp outputs.
                # No .copy(): none of the `cb_*` functions mutates this
                # DataFrame in place; every modification happens on DataFrames
                # produced by subsequent merge/assign calls.
                data = self.ssp_data
        

            #time_period = range(SSP_GLOBAL_TIME_PERIODS)
            implementation = [0]*11 + list(np.linspace(0, 0.95, SSP_GLOBAL_TIME_PERIODS - 11))

            manure_imp = pd.DataFrame({"implementation" : implementation})

            tlus = self.cb_get_data_from_wide_to_long(data, cb_orm.strategy_code_tx, cb_orm.diff_var)
            tlus = pd.concat([tlus, manure_imp], axis = 1)
            tlus["difference_variable"] = cb_orm.diff_var
            tlus["difference_value"] = tlus["value"]*tlus["implementation"]
            tlus["value"] = tlus["difference_value"]*cb_orm.multiplier
            tlus["variable"] = cb_orm.output_variable_name
            
            ## Used to compute the difference between the baseline strategy and the pathway.
            ## In this case the baseline value is set to zero.
            tlus['variable_value_baseline'] = 0
            tlus['variable_value_pathway'] = tlus["difference_value"]

            tlus = tlus[SSP_GLOBAL_COLNAMES_OF_RESULTS]

            return tlus.dropna()

        else:
            print(f"The variable {cb_orm.output_variable_name} cannot be computed with the cb_manure_management_cost function" + f"\nYou must use the {cb_orm.cb_function} function instead")
            return None
