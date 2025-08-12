from costs_benefits_ssp.cb_calculate import CostBenefits
import pandas as pd 
import os 

# Set directory paths
DIR_PATH = os.path.dirname(os.path.abspath(__file__))
TEST_RUNS_RESULTS_PATH = os.path.join(DIR_PATH, "test_runs_results")
REGION_RESULTS_PATH = os.path.join(TEST_RUNS_RESULTS_PATH, "mexico")
build_path = lambda PATH  : os.path.abspath(os.path.join(*PATH))
SSP_RESULTS_PATH = build_path([REGION_RESULTS_PATH,"ssp_data"])

# Directory for cost table configuration
CB_DEFAULT_DEFINITION_PATH = build_path([REGION_RESULTS_PATH, "cost_factors"])

# Output directory for cost and benefit module results
OUTPUT_CB_PATH = build_path([REGION_RESULTS_PATH, "cb_results"])
os.makedirs(OUTPUT_CB_PATH, exist_ok=True)

# Directory for data required to generate the tornado_plot_data_QA_QC.csv file
QA_PATH = build_path([DIR_PATH, "edgar_cw"])

# Load data
ssp_data = pd.read_csv(os.path.join(SSP_RESULTS_PATH, "sisepuede_results_sisepuede_run_2025-02-11T11;37;41.739098_WIDE_INPUTS_OUTPUTS.csv"))
att_primary = pd.read_csv(os.path.join(SSP_RESULTS_PATH, "ATTRIBUTE_PRIMARY.csv"))
att_strategy = pd.read_csv(os.path.join(SSP_RESULTS_PATH, "ATTRIBUTE_STRATEGY.csv"))

#ssp_data = ssp_data.drop(columns = ["totalvalue_enfu_fuel_consumed_inen_fuel_hydrogen", "totalvalue_enfu_fuel_consumed_inen_fuel_furnace_gas"])

# Define the baseline strategy
strategy_code_base = "BASE"

# Instantiate an object of the CostBenefits class
cb = CostBenefits(ssp_data, att_primary, att_strategy, strategy_code_base)

# The export_db_to_excel method saves the initial configuration of the cost tables to an Excel file.
# Each sheet represents a table in the cost and benefits program database.
CB_DEFAULT_DEFINITION_FILE_PATH = os.path.join(CB_DEFAULT_DEFINITION_PATH, "cb_config_params.xlsx")

# CHECK IF THE FILE EXISTS
if not os.path.exists(CB_DEFAULT_DEFINITION_FILE_PATH):
    print(f"File {CB_DEFAULT_DEFINITION_FILE_PATH} does not exist. Please check the path or create the file.")
else:
    print(f"File {CB_DEFAULT_DEFINITION_FILE_PATH} exists. Proceeding with loading parameters.")

#cb.export_db_to_excel(CB_DEFAULT_DEFINITION_FILE_PATH)
cb.load_cb_parameters(CB_DEFAULT_DEFINITION_FILE_PATH)

#------ System Costs
# Calculate system costs for all strategies
results_system = cb.compute_system_cost_for_all_strategies(verbose=True)

#-------Technical Costs
# Calculate technical costs for all strategies
results_tx = cb.compute_technical_cost_for_all_strategies(verbose=True)

# Combine results
results_all = pd.concat([results_system, results_tx], ignore_index = True)

#-------------POST PROCESS SIMULATION RESULTS---------------
# Post process interactions among strategies that affect the same variables
results_all_pp = cb.cb_process_interactions(results_all)
