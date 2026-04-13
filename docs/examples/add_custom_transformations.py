"""
Ejemplo: cómo añadir transformations personalizadas al paquete
costs_benefits_ssp para que el cálculo de costos/beneficios las tome en
cuenta.

Aplica al caso de Libia donde ATTRIBUTE_STRATEGY.csv contiene transformations
(FRST:INCREASE_SEQUESTRATION, LNDU:BOUND_CLASSES, LNDU:DEC_CLASS_LOSS,
ENFU:ADJ_EXPORTS, SCOE:INC_EFFICIENCY_HEAT) que no vienen por defecto en la
tabla attribute_transformation_code del paquete.

Hay DOS niveles:

(A) SOLO registrar la TX en la tabla `attribute_transformation_code`.
    Con esto los checks `tx in strategy_to_txs[strategy_code]` reconocen la
    TX, pero NO se computa ningún costo/beneficio para ella. Útil si la TX
    existe en SISEPUEDE pero todavía no tienes factores económicos.

(B) Registrar la TX + añadir el factor de costo/beneficio correspondiente.
    Necesitas además una fila en `tx_table` (con cost_type) y una en
    `transformation_costs` (o `cost_factors` para system costs).

IMPORTANTE: los valores de `multiplier`, `difference_variable` y
`cb_function` son PLACEHOLDERS. Debes reemplazarlos con los factores
económicos reales de tu contexto (por ejemplo, sacados del Appendix del
reporte de SISEPUEDE o de tu propia investigación).
"""
import os
import pandas as pd

from costs_benefits_ssp.cb_calculate import CostBenefits


# ---------------------------------------------------------------
# 1) Carga tus datos igual que en test.py
# ---------------------------------------------------------------
SSP_RESULTS_PATH = "/ruta/a/tu/ssp_salidas"

ssp_data = pd.read_csv(os.path.join(SSP_RESULTS_PATH, "libya.csv"))
att_primary = pd.read_csv(os.path.join(SSP_RESULTS_PATH, "ATTRIBUTE_PRIMARY.csv"))
att_strategy = pd.read_csv(os.path.join(SSP_RESULTS_PATH, "ATTRIBUTE_STRATEGY.csv"))

cb = CostBenefits(ssp_data, att_primary, att_strategy, "BASE")

# Al instanciar verás un UserWarning listando las TX faltantes.
# También están disponibles en:
print("TX faltantes:", cb.missing_transformation_codes)


# ---------------------------------------------------------------
# 2) NIVEL A — registrar las TXs en attribute_transformation_code
# ---------------------------------------------------------------
# Campos:
#   - transformation_code: identificador único (PK). Debe coincidir con el
#     prefijo que aparece en tu ATTRIBUTE_STRATEGY.csv.
#   - transformation: nombre legible para humanos.
#   - transformation_id: id numérico/texto opcional.
#   - sector: sector al que pertenece (FRST, LNDU, ENFU, SCOE, etc.).
#   - description: descripción libre.

new_transformation_codes = [
    {
        "transformation_code": "TX:FRST:INCREASE_SEQUESTRATION",
        "transformation": "Increase forest sequestration",
        "transformation_id": "FRST_001",
        "sector": "FRST",
        "description": "Incremento del secuestro de carbono en bosques",
    },
    {
        "transformation_code": "TX:LNDU:BOUND_CLASSES",
        "transformation": "Bound land use classes",
        "transformation_id": "LNDU_001",
        "sector": "LNDU",
        "description": "Restringe transiciones entre clases de uso de suelo",
    },
    {
        "transformation_code": "TX:LNDU:DEC_CLASS_LOSS",
        "transformation": "Decrease class loss",
        "transformation_id": "LNDU_002",
        "sector": "LNDU",
        "description": "Reduce la pérdida de clases de uso de suelo",
    },
    {
        "transformation_code": "TX:ENFU:ADJ_EXPORTS",
        "transformation": "Adjust fuel exports",
        "transformation_id": "ENFU_001",
        "sector": "ENFU",
        "description": "Ajuste de exportaciones de combustible",
    },
    {
        "transformation_code": "TX:SCOE:INC_EFFICIENCY_HEAT",
        "transformation": "Increase heat efficiency in stationary combustion",
        "transformation_id": "SCOE_001",
        "sector": "SCOE",
        "description": "Mejora de eficiencia térmica en combustión estacionaria",
    },
]

cb.insert_cb_records("attribute_transformation_code", new_transformation_codes)


# Si SÓLO quieres que estas TXs sean reconocidas (sin computar costo), ya
# está: vuelve a llamar `cb.get_strategy_to_txs(att_strategy)` o simplemente
# re-instancia el objeto. En la mayoría de casos con solo registrar las TX es
# suficiente si el objetivo es evitar warnings.


# ---------------------------------------------------------------
# 3) NIVEL B — añadir también el factor de costo/beneficio
# ---------------------------------------------------------------
# Para que una TX GENERE costos, necesitas:
#   (i)   Una entrada en `tx_table`: define el nombre de la variable de
#         salida (output_variable_name) y su cost_type ("transformation_cost"
#         o "system_cost").
#   (ii)  Si cost_type="transformation_cost", una entrada en
#         `transformation_costs` con el cb_function, difference_variable,
#         multiplier, etc.
#         Si cost_type="system_cost", una entrada en `cost_factors`.
#
# Las funciones cb_function soportadas son las listadas en
# `mapping_strategy_specific_functions`:
#   - cb_difference_between_two_strategies
#     (el caso genérico: costo = multiplier × (valor_tx − valor_base))
#   - cb_apply_cost_factors  (alias de la anterior)
#   - cb_scale_variable_in_strategy
#     (costo = multiplier × valor_en_la_estrategia, sin baseline)
#   - cb_fraction_change
#   - cb_entc_reduce_losses
#   - cb_ippu_clinker
#   - cb_ippu_florinated_gases
#   - cb_fgtv_abatement_costs
#   - cb_waso_reduce_consumer_facing_food_waste
#   - cb_lvst_enteric
#   - cb_agrc_rice_mgmt
#   - cb_agrc_lvst_productivity
#   - cb_pflo_healthier_diets
#   - cb_ippu_inen_ccs
#   - cb_manure_management_cost
#
# Para la mayoría de TXs nuevas, `cb_difference_between_two_strategies` es
# suficiente: toma la diferencia de una variable SSP entre la estrategia de
# interés y la baseline y la multiplica por un factor económico.

# --- EJEMPLO: costo técnico para TX:SCOE:INC_EFFICIENCY_HEAT ---
# Supongamos que quieres capturar el ahorro de energía térmica como un
# beneficio proporcional a la reducción de consumo de combustible en SCOE.
# Necesitas:
#   - un output_variable_name único (convención: "cb:<sector>:<tipo>:<detalle>")
#   - la variable SSP cuya diferencia quieres multiplicar
#   - un multiplier (USD por unidad de esa variable; negativo = beneficio)

tx_table_new = [
    {
        "output_variable_name": "cb:scoe:technical_savings:heat_efficiency",
        "output_display_name": "SCOE heat efficiency savings",
        "internal_notes": "Placeholder: reemplazar multiplier con factor real",
        "display_notes": "Ahorros por eficiencia térmica en SCOE",
        "cost_type": "transformation_cost",
    },
]

transformation_costs_new = [
    {
        "output_variable_name": "cb:scoe:technical_savings:heat_efficiency",
        "transformation_code": "TX:SCOE:INC_EFFICIENCY_HEAT",
        "include": True,
        "include_variant": 0,
        "test_id_variant_suffix": "",
        "comparison_id_variant": "",
        "cb_function": "cb_difference_between_two_strategies",
        # Variable SSP de referencia: ajusta al nombre real que tengas en
        # tu CSV de salidas (puede llevar wildcards `*` o alternativas `a|b`).
        "difference_variable": "energy_demand_scoe_total_fuel_*",
        # Placeholder: reemplaza con tu factor real (USD por PJ, por ejemplo).
        # Negativo porque es un ahorro (beneficio).
        "multiplier": -10.0,
        "multiplier_unit": "USD/PJ",
        # Tasa de cambio anual del factor a partir de 2023 (1.0 = constante).
        "annual_change": 1.0,
        "arg1": None,
        "arg2": None,
        # Si sum=1 se suman todas las diff_vars que matcheen el patrón.
        "sum": True,
        "natural_multiplier_units": "USD/PJ",
    },
]

cb.insert_cb_records("tx_table", tx_table_new)
cb.insert_cb_records("transformation_costs", transformation_costs_new)


# Repite el mismo patrón para las demás TXs que quieras costear.
# Ejemplo análogo para FRST:INCREASE_SEQUESTRATION (beneficio = $USD por
# tonelada de CO2e secuestrada).

tx_table_new2 = [
    {
        "output_variable_name": "cb:frst:benefit:carbon_sequestration",
        "output_display_name": "Forest carbon sequestration benefit",
        "internal_notes": "Placeholder",
        "display_notes": "Valor del CO2 secuestrado",
        "cost_type": "transformation_cost",
    },
]

transformation_costs_new2 = [
    {
        "output_variable_name": "cb:frst:benefit:carbon_sequestration",
        "transformation_code": "TX:FRST:INCREASE_SEQUESTRATION",
        "include": True,
        "include_variant": 0,
        "test_id_variant_suffix": "",
        "comparison_id_variant": "",
        "cb_function": "cb_difference_between_two_strategies",
        "difference_variable": "emission_co2e_co2_lndu_forests_*",  # ajusta
        # USD por tonelada CO2e evitada (precio social del carbono, placeholder)
        "multiplier": -40.0,
        "multiplier_unit": "USD/tCO2e",
        "annual_change": 1.0,
        "arg1": None,
        "arg2": None,
        "sum": True,
        "natural_multiplier_units": "USD/tCO2e",
    },
]

cb.insert_cb_records("tx_table", tx_table_new2)
cb.insert_cb_records("transformation_costs", transformation_costs_new2)


# ---------------------------------------------------------------
# 4) Corre el pipeline con la nueva configuración
# ---------------------------------------------------------------
results_system = cb.compute_system_cost_for_all_strategies(verbose=False)
results_tx = cb.compute_technical_cost_for_all_strategies(verbose=False)

results_all = pd.concat([results_system, results_tx], ignore_index=True)
results_all_pp = cb.cb_process_interactions(results_all)
results_final = cb.cb_shift_costs(results_all_pp)

results_final.to_csv("cba_resultados_libia.csv", index=False)


# ---------------------------------------------------------------
# 5) (OPCIONAL) Guarda la configuración ampliada a Excel
# ---------------------------------------------------------------
# Así la próxima vez no necesitas este script: basta con load_cb_parameters.
cb.export_db_to_excel("cb_config_params_libia.xlsx")
