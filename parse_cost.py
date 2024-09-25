# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 14:49:15 2023
-*- coding: utf-8 -*-
"""

import pandas as pd
import numpy as np
import xlwings as xw

from pathlib import Path
STATE_SHEET = "State Inputs"
BLOCK_START_ROWS = [0, 50, 99, 148, 197, 246]


def get_year_range() -> np.ndarray:
    """
    :return: A numpy array of integers ranging from -1 to 41 (inclusive) with 43 evenly spaced elements.
    """
    return np.linspace(-1, 41, 43).astype(int)


def extract_cost_info(original: pd.DataFrame, start_row: int, cz_column: int) -> list[float]:
    """
    :param original: The input DataFrame from which cost information is extracted.
    :param start_row: The starting row index from where the extraction begins.
    :param cz_column: The column index which contains the cost information.
    :return: A list of extracted cost information from the specified rows and column.
    """
    return (
            [original.iloc[start_row + 3, cz_column]] +
            [original.iloc[start_row + 2, cz_column]] +
            list(original.iloc[start_row + 5:start_row + 45, cz_column]) +
            [original.iloc[start_row + 46, cz_column]]
    )


def process_device_type(original: pd.DataFrame, building: str,
                        year_range: np.ndarray, start_row: int,
                        dev_start: int, device_type: str) -> list[pd.DataFrame]:
    """
    :param original: DataFrame containing the original data from which information is to be extracted.
    :param building: String representing the building identifier or name.
    :param year_range: NumPy array representing the range of years for which data is to be processed.
    :param start_row: Integer indicating the starting row in the original DataFrame for data extraction.
    :param dev_start: Integer indicating the starting column index for device type information.
    :param device_type: String representing the type of device for which cost information is to be processed.
    :return: List of DataFrames, each containing processed cost information categorized by building, year, climate zone, and device type.
    """
    frames = []
    for x in range(0, 5):
        cz_column = dev_start + x
        climate_zone = str(original.iloc[start_row + 1, cz_column]).strip()
        if climate_zone != '0.0':
            cost_info = extract_cost_info(original, start_row, cz_column)
            frame = pd.DataFrame({
                'Building': building,
                'Year': year_range,
                'ClimateZone': climate_zone,
                'DeviceType': device_type,
                'Cost': cost_info
            })
            frames.append(frame)
    return frames


def create_frame(original: pd.DataFrame) -> pd.DataFrame:
    """
    :param original: The original DataFrame containing building data.
    :return: A new DataFrame with the processed device type data, where the index is set to 'Building', 'Year', 'DeviceType', and 'ClimateZone'.
    """
    frame_list = []
    year_range = get_year_range()
    for start_row in BLOCK_START_ROWS:
        building = original.iloc[start_row, 0]
        for dev_start, device_type in zip([1, 6, 13, 18], ['HVAC', 'Lighting', 'Envelope', 'Total']):
            frame_list.extend(process_device_type(original, building, year_range, start_row, dev_start, device_type))
    new_frame = pd.concat(frame_list, axis=0)
    new_frame.set_index(['Building', 'Year', 'DeviceType', 'ClimateZone'], inplace=True)
    return new_frame



class Worker:
    def __init__(self, file_path, output_dir):
        self.wkbk = xw.Book(file_path)
        self.states = self.wkbk.sheets[STATE_SHEET]
        options = self.states.range('A4').api.Validation.Formula1[1:]
        self.states_list = [item.value for item in self.states.range(options) if item.value is not None]
        self.state_abbr_list = [item.value for item in self.states.range('B9:B60')]
        self.state_df = {}
        self.output_dir = output_dir

    def make_dict_df(self, state: str) -> pd.DataFrame:
        """
        Create state level dictionary for proto buildings DataFrame.

        :param state: Name of current state
        :return state_df: dictionary of building data frames for each state.
        """
        try:
            self.states.range('A4').value = state
        except (KeyError, AttributeError):
            print('Snap, we suck!')
            raise
        df = self.wkbk.sheets('Cost Est Summary')
        df = df[f'B20:X312'].options(pd.DataFrame, index=False, header=False).value
        modified_df = create_frame(df)
        return modified_df

    def store_files(self):
        """Output state/building info to file"""
        for state_name, state_df in self.state_df.items():
            state_df.to_csv(self.output_dir / f'{state_name}.csv')

    def work_main(self):
        try:
            for state in self.states_list:
                try:
                    self.state_df[state] = self.make_dict_df(state)
                except Exception as ex:
                    print(f'Error for {state} -- {ex}!')
        finally:
            self.wkbk.save()
            self.wkbk.close()


######################################################################
# Configuration of script.
OUTPUT_DIRECTORY = 'cost_data_CE/2010'
XLSM_FILE_PATH = 'inputs/901-10_State_CE_Analysis_082024.xlsm'
######################################################################

def configure_script(output_dir, file_path):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    return file_path, output_path


if __name__ == '__main__':
    xlsm_file, output_directory = configure_script(OUTPUT_DIRECTORY, XLSM_FILE_PATH)

    worker = Worker(xlsm_file, output_directory)
    worker.work_main()
    worker.store_files()
