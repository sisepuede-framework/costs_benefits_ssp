from sisepuede.core.model_attributes import is_model_attributes
from typing import *
import costs_benefits_ssp.cb_calculate as cbc
import numpy as np
import pandas as pd
import pathlib
import sisepuede.utilities._toolbox as sf




class CBSSPWrapperForDFComparison:
    """Build a wrapper for CBA to allow it to work for now to compare strategies
        without the entire SSP infrastructure. Needs a wide (input/output) 
        dataframe with a strategy id column + a strategy attribute table. 
        
    Doesn't reload the DB interface each time. Requies SISEPUEDE.

    """

    def __init__(self,
        model_attributes: 'ModelAttributes',
        path_cb_config: pathlib.Path,
    ) -> None:

        self.cb_obj = None
        
        self._initialize_fields()
        self._initialize_cb_objs(path_cb_config, )
        self._initialize_model_attributes(model_attributes, )
        
        return None



    def _initialize_cb_objs(self,
        path_cb_config: pathlib.Path,               
    ) -> None:
        """initialize some cost benefit things
        """

        if not path_cb_config.is_file():
            raise RuntimeError(f"No config file found at path '{path_cb_config}'.")


        ##  SET PROPERTIES

        self.cb_obj = None
        self.path_cb_config = path_cb_config

        return None

        
        
    def _initialize_fields(self,
    ) -> None:
        """initialize some fields to use
        """

        ##  SET PROPERTIES

        self.field_cb_type = "cb_type"
        self.field_description = "description"
        self.field_item_1 = "item_1"
        self.field_item_2 = "item_2"
        self.field_name = "name"
        self.field_sector = "sector"
        self.field_strategy = "strategy"
        self.field_strategy_code = "strategy_code"
        self.field_technical_cost = "technical_cost"
        self.field_transformations_specification = "transformation_specification"
        self.field_value = "value"
        self.field_variable = "variable"
        
        return None


        
    def _initialize_model_attributes(self,
        model_attributes: 'ModelAttributes',
    ) -> None:
        """Check the ModelAttributes object and associated derivatives
        """

        if not is_model_attributes(model_attributes, ):
            raise TypeError("model_attributes must be a ModelAttributes object.")

        # set default base code
        code_strategy_base_default = "BASE"

        
        ##  SET PROPERTIES

        self.code_strategy_base_default = code_strategy_base_default
        self.key_design = model_attributes.dim_design_id
        self.key_future = model_attributes.dim_future_id
        self.key_primary = model_attributes.dim_primary_id
        self.key_strategy = model_attributes.dim_strategy_id
        self.key_time_period = model_attributes.dim_time_period
        self.model_attributes = model_attributes

        return None



    
    ###############################
    #    PRIMARY FUNCTIONALITY    #
    ###############################

    def _build_dummy_primary(self, 
        attr_strat: pd.DataFrame,
    ) -> pd.DataFrame:
        """From a strategy table, build a dummy
        """
        
        vals = sorted(attr_strat[self.key_strategy].unique())

        """
        odpt = OrderedDirectProductTable(
            {
                self.key_design: [0],
                self.key_future: [0],
                self.key_strategy: vals,
            },
            [
                self.key_primary,
                self.key_design,
                self.key_strategy,
                self.key_future,
            ],
            key_primary = self.key_primary,
        )

        out = odpt.get_indexing_dataframe({self.key_strategy: vals, })
        """
        zeros = np.zeros(len(vals), int)
        
        out = pd.DataFrame(
            {
                self.key_primary: vals,
                self.key_design: zeros,
                self.key_strategy: vals,
                self.key_future: zeros,
            }
        )
        
        return out



    def _build_strategy_maps(self,
        att_primary: pd.DataFrame,
        att_strategy: pd.DataFrame,
    ) -> tuple:
        """
        Build (strategy_id_map, primary_id_map) dicts keyed by strategy_code.
    
        strategy_id_map  : strategy_code -> strategy_id  (from att_strategy)
        primary_id_map   : strategy_code -> primary_id   (from att_primary join)
        """
        
        sid_map = sf.build_dict(
            att_strategy[
                [self.field_strategy_code, self.key_strategy]
            ]
        )

        sid_to_pid = sf.build_dict(
            att_primary[
                [self.key_strategy, self.key_primary]
            ]
        )
        
        pid_map = {code: sid_to_pid.get(sid) for code, sid in sid_map.items()}
        out = (
            sid_map, 
            pid_map,
        )

        return out
    
        

    def _calculate(self,
        df_wide: pd.DataFrame,
        attr_strat: pd.DataFrame,         
        code_strat_base: Union[str, None] = None,
    ) -> pd.DataFrame:
        """Calculate costs and benefits and return a table
        """

        ##  INITIALIZE
        
        # update the cb_obj
        attr_prim = self._update_cb_components(
            df_wide,
            attr_strat,
            code_strat_base = code_strat_base,
        ) 

        # get some maps
        strategy_id_map, primary_id_map = self._build_strategy_maps(attr_prim, attr_strat, )
        

        ##  CALCULATE
        
        # compute
        df_system = self.cb_obj.compute_system_cost_for_all_strategies(verbose = False, )
        df_tech = self.cb_obj.compute_technical_cost_for_all_strategies(verbose = False,)
        df_all = pd.concat([df_system, df_tech], ignore_index = True, )
        
        # post-process interactions and shift pre-2025 costs NOTE THIS NEEDS TO BE FIXED
        df_all = self.cb_obj.cb_process_interactions(df_all, )
        df_all = self.cb_obj.cb_shift_costs(df_all)
        
        # reshape and add some columns    
        out = self._reshape(
            df_all,
            strategy_id_map,
            primary_id_map,
        ) 

        return out



    def _get_code_strat_base(self,
        code_strat_base: Union[str, None],
    ) -> str:
        """Get the base strategy code
        """
        if not isinstance(code_strat_base, str):
            return self.code_strategy_base_default

        return code_strat_base
        
    
    
    def _reshape(self,
        results_shifted: pd.DataFrame,
        strategy_id_map: dict,
        primary_id_map: dict,
    ) -> pd.DataFrame:
        """Reshape cost-benefit results to Tableau-ready format.
    
        Steps
        -----
        1.  Split variable string into (name, sector, cb_type, item_1, item_2).
        2.  Scale USD → billions USD.
        3.  Drop pre-2025 shifted rows.
        4.  Add Year, strategy label, strategy_id, primary_id, ids, gdp_mmm_usd.
        """
        cb = results_shifted.copy()
    
        # 1. Decompose variable
        parts = cb[self.field_variable].astype(str).str.split(":", n=4, expand=True)
        parts.columns = [
            self.field_variable, 
            self.field_sector, 
            self.field_cb_type, 
            self.field_item_1,
            self.field_item_2
        ]
        
        cb = pd.concat(
            [
                cb, 
                parts
                .drop(
                    columns = [
                        self.field_variable,
                    ]
                )
            ], 
            axis = 1,
        )
    
        # 2. USD → billions
        cb[self.field_value] = cb[self.field_value] / 1e9
    
        # 3. Remove shifted entries (costs that were redistrubuted pre-2025)
        self.cb = cb
        cb = cb[~cb[self.field_item_2].astype(str).str.contains("shifted", na=False)]
        cb = cb[~cb[self.field_variable].astype(str).str.contains("shifted2", na=False)]
    
        # 4. year
        cb = pd.merge(
            cb,
            self.model_attributes.get_dimensional_attribute_table(
                self.model_attributes.dim_time_period
            ).table,
            how = "left",
        )
    
        # 5. Strategy metadata from the maps (dynamic — no hardcoding)
        cb[self.key_strategy] = cb[self.field_strategy_code].map(strategy_id_map)
        cb[self.key_primary]  = cb[self.field_strategy_code].map(primary_id_map)

        # 6. Unique identifier
        cb[self.field_variable] = [
            x.replace(":", "_") for x in 
            cb[self.field_variable].astype(str).values
        ]
    
        # 7. TEMP PATCH: drop ENTC technical-cost rows with the wrong sign
        #    (capex, transmission, ...). Costs are stored negative (Tableau plots
        #    SUM(value)*-1), so value > 0 here renders as a *negative* cost in the
        #    dashboard, which is not meaningful. Remove until the upstream ENTC
        #    technical-cost calculation is fixed.
        bad_sign = (
            (cb[self.field_sector] == "entc")
            & (cb[self.field_cb_type] == self.field_technical_cost)
            & (cb[self.field_value] > 0)
        )
        if bad_sign.any():
            """
            dropped = cb.loc[bad_sign, [self.field_variable, self.field_strategy_code, "Year"]]
            print(
                f"[cb_pipeline] TEMP PATCH: dropping {bad_sign.sum()} wrong-sign "
                "ENTC technical-cost rows:"
            )
            for var, grp in dropped.groupby(self.field_variable):
                years = ", ".join(
                    f"{s} {y}"
                    for s, y in grp[[self.field_strategy_code, "Year"]].itertuples(index=False, name=None)
                )
                print(f"    {var}: {years}")
            """
            cb = cb[~bad_sign].copy()


        ##  CLEAN UP

        fields_ind = [
            self.key_strategy,
            self.key_time_period,
            self.field_variable,
        ]
    
        cb = (
            cb
            .get(fields_ind + [self.field_value])
            .groupby(fields_ind)
            .sum()
            .reset_index()
        )

        cb = (
            sf.pivot_df_clean(
                cb,
                [self.field_variable],
                [self.field_value]
            )
            .sort_values(
                by = [
                    self.key_strategy,
                    self.key_time_period
                ]
            )
            .reset_index(drop = True, )
        )
        
        return cb


        
    def _update_cb_components(self, 
        df_wide: pd.DataFrame,
        attr_strat: pd.DataFrame,
        code_strat_base: Union[str, None] = None,
    ) -> pd.DataFrame:
        """Using a strategy table, (a) build a dummy primary id table and (b) update the 
            elements in the cb object without rebuilding (avoids loading CB data again, 
            which is bothering me with a completely fucking inane db structure).

        Returns the primary attribute table.

        Function Arguments
        ------------------
        df_wide : pd.DataFrame
            DataFrame storing input and output results by strategy_id
        attr_strat : pd.DataFrame
            Strategy attributes storing id, name, code, and transformation_specification
        """

        # note that, in the dummy, primary and strategy are the same
        attr_prim = self._build_dummy_primary(attr_strat, )
        code_strat_base = self._get_code_strat_base(code_strat_base, )

        ##  CHECK IF cb_obj IS ALREADY DEFINED
        #       - If not, create
        #       - Once one is found (incl. after creation), update dataframe
        if self.cb_obj is None:
            cb_obj = cbc.CostBenefits(
                self._dummy_df_conversion(df_wide, ), 
                attr_prim, 
                attr_strat, 
                code_strat_base,
            )

            # load factors and assign property
            cb_obj.load_cb_parameters(str(self.path_cb_config, ))
            self.cb_obj = cb_obj

        # update the data in the cb object
        self.cb_obj._initialize_ssp_data(
            self._dummy_df_conversion(df_wide, ), 
            attr_prim,
            attr_strat,
            code_strat_base,
        ) 
        
        return attr_prim



    def _dummy_df_conversion(self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Rename strategy to primary in input df
        """
        df_out = df.rename(
            columns = {
                self.key_strategy: self.key_primary,
            }
        )

        return df_out