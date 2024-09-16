# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 14:49:15 2023

"""

import pandas as pd
import numpy as np
import us
import xlwings as xw

from pathlib import Path
STATE_SHEET = "State Inputs"


def create_frame(original):
    block_start_rows = [0, 50, 99, 148, 197, 246]
    frames = []
    for start_row in block_start_rows:
        building = original.iloc[start_row, 0]
        year =  np.linspace(-1, 41, 43).astype(int)
        for dev_start, device_type in zip([1, 6, 13, 18], ['HVAC', 'Lighting', 'Envelope', 'Total']):
            for x in range(0, 5):
                cz_column = dev_start + x
                cz = str(original.iloc[start_row+1, cz_column]).strip()
                if cz != '0.0':
                    cost = [original.iloc[start_row+3, cz_column]] + [original.iloc[start_row+2, cz_column]] + list(original.iloc[start_row+5:start_row+45, cz_column]) + [original.iloc[start_row+46, cz_column]]
                    frame = pd.DataFrame({'Building': building, 'Year': year, 'ClimateZone': cz, 'DeviceType': device_type, 'Cost': cost})
                    frames.append(frame)
    new_frame = pd.concat(frames, axis=0)
    new_frame.set_index(['Building', 'Year', 'DeviceType', 'ClimateZone'], inplace=True)
    return new_frame


class Worker:
    def __init__(self, file_path, output_dir):
        self.wkbk = xw.Book(file_path)
        self.states = self.wkbk.sheets[STATE_SHEET]
        options = self.states.range('A4').api.Validation.Formula1[1:]
        self.states_list = [item.value for item in self.states.range(options) if item.value is not None]
        self.current_state = self.states.range('A4').value
        print(f'Current state: {self.current_state}')
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
            #current_state_abbr = us.states.lookup(state).abbr
        except (KeyError, AttributeError):
            print('Snap, we suck!')
            raise
        print(f'New state {state}')
        df = self.wkbk.sheets('Cost Est Summary')
        df = df[f'B20:X312'].options(pd.DataFrame, index=False, header=False).value
        modified_df = create_frame(df)
        return modified_df

    def store_files(self):
        """Output state/building info to file"""
        # cost_path = Path(self.output_dir)
        # cost_path.mkdir(parents=True, exist_ok=True)
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


# output_dir = 'C:/Users/vlac284/OneDrive - PNNL/Alex-Robert-Matt-state/working_versions_2024_08_05/cost_data_CE/2010'
# xlsm_file_path = 'C:/Users/vlac284/OneDrive - PNNL/Alex-Robert-Matt-state/working_versions_2024_08_05/901-10_State_CE_Analysis_082024.xlsm'
# worker = Worker(xlsm_file_path, output_dir)
# worker.work_main()
# worker.store_files()
