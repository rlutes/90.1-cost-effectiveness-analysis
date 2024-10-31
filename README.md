
# Cost Effectiveness (CE) Model

The Cost Effectiveness (CE) model is a Microsoft Excel file (`.xlsm`) that contains data on the cost of energy measures for mechanical systems, lighting, envelope retrofits, and more. These models, spanning from 2010 to 2022, implement energy measures from ASHRAE 90.1. The following models are included:

1. `901-10_State_CE_Analysis_082024.xlsm`
2. `901-13_State_CE_Analysis_082024.xlsm`
3. `901-16_State_CE_Analysis_082024.xlsm`
4. `901-19_State_CE_Analysis_082024.xlsm`
5. `901-22_State_CE_Analysis_082024.xlsm`

In each model, the year after "901" represents the target year for the energy measures, with a base year set three years prior. For example, in `901-10_State_CE_Analysis_082024.xlsm`, the target year is 2010, and the base year is 2007.

## Scripts and Instructions

### 1. Running `parse_HVAC.py`

- **Description:** This script processes HVAC data from the CE models located in the `inputs` folder.
- **Functionality:**
  - Updates the drop-down in cell `A4` of the "State Inputs" sheet.
  - Extracts and parses data for HVAC measures (e.g., "90.1-2010bi heat pump efficiency").
  - Parses rows marked with an "X" in column A and retrieves data from the "maintenance," "replacement life," and "total replacement cost" columns.
- **Output:**
  - Creates a main folder `hvac_data_CE` with subfolders for each target year (`2010`, `2013`, `2016`, `2019`, `2022`).
  - Each subfolder contains parsed data for HVAC in CSV format, named `{state}_HVAC_{building type}.csv`.

### 2. Running `parse_cost.py`

- **Description:** This script processes cost data from the CE models located in the `inputs` folder.
- **Functionality:**
  - Updates the drop-down in cell `A4` of the "State Inputs" sheet.
  - Extracts cost data for building types like Small Office, Large Office, Standalone Retail, etc.
  - Copies data from the "lighting" and "Envelope Power and Other" columns.
- **Output:**
  - Creates a main folder `cost_data_CE` with subfolders for each target year (`2010`, `2013`, `2016`, `2019`, `2022`).
  - Each subfolder contains parsed cost data in CSV format, named `{state}.csv`.

### 3. Running `parse_all.py`

- **Description:** This script combines the `parse_HVAC.py` and `parse_cost.py` scripts.
- **Functionality:** Runs both parsing scripts sequentially.

### 4. Running `assemble_hvac_cost.py`

- **Description:** This script aggregates HVAC data across base and target years.
- **Functionality:**
  - Reads from the `current_vs_target_master.csv` file.
  - Aggregates data based on base and target year ranges, e.g., combining years [2016, 2019] for base and [2019, 2022] for target.
- **Output:** Aggregated data for each state, organized by base and target years.

### 5. Running `assemble_light_envelope_cost.py`

- **Description:** This script aggregates lighting and envelope cost data across target years.
- **Functionality:**
  - Reads from the `current_vs_target_master.csv` file.
  - Aggregates cumulative data for target years, e.g., combining [2019, 2022] for a given state.
- **Output:** A single CSV with columns for State, Building, CodeYear, DeviceType, Year, and Climate Zones.