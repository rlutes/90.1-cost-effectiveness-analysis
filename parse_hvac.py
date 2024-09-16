# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 14:49:15 2023

"""
# look for the comments "Alex updated"

import pandas as pd
import threading
import numpy as np
import us
from random import randint
import xlwings as xw
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from pathlib import Path

data_map = {1: 'R', 2: 'AB', 3: 'AL', 4: 'AV', 5: 'BF'}
STATE_SHEET = "State Inputs"
BUILDINGS = [
    "HVAC Small Office Proto",
    "HVAC Large Office Proto",
    "HVAC Standalone Retail Proto",
    "HVAC Primary School Proto",
    "HVAC Small Hotel Proto",
    "HVAC Mid-rise Apartment Proto"
]


def stringify(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return df with None removed from column index
    """
    columns = [str(item) for item in df.columns]
    df.columns = columns
    return df


def find_start_columns(df):
    """Returns column indices where blocks begin"""
    return [i for i, n in enumerate(df.columns) if 'Code' in n]


def find_end_columns(df):
    """Returns column indicies where a block may end.
        Note that the return may have extra elements, so long as all real end sites are included.
    """
    return [i for i, n in enumerate(df.columns) if 'Code' in n or 'Climate Zone' in n] + [len(df.columns)]


def find_climate_zone_columns(df):
    """Returns column indices where a Climate Zone label is found."""
    return [i for i, n in enumerate(df.columns) if 'Climate Zone' in n]


def find_headers(df):
    """Returns formatted headers for all columns in the original dataframe."""
    headers1 = df.iloc[0, :].fillna('')
    headers2 = df.iloc[1, :].fillna('')
    return [' '.join([str(headers1.iloc[i]).strip(), str(headers2.iloc[i]).strip()]) for i in range(len(headers1))]


def create_frame(original, block_start_func, block_end_func, climate_zone_func, header_func, header_row_count):
    measure_column = original.pop('Measure')
    original = stringify(original)
    block_start_columns = block_start_func(original)
    block_end_columns = block_end_func(original)
    headers = header_func(original)
    climate_zone_columns = climate_zone_func(original)
    frames = []
    for block_start in block_start_columns:
        zone = next(original.columns[i+1] for i in reversed(climate_zone_columns) if i < block_start)
        year = original.columns[block_start+1].split('.')[0]
        block_end = next(i for i in block_end_columns if i > block_start)
        frame = original.iloc[header_row_count-1:, block_start:block_end]
        frame.columns = headers[block_start:block_end]
        frame.insert(0, 'Measure', measure_column)
        frame.insert(1, 'Climate Zone', zone)
        frame.insert(2, 'Year', year)
        frames.append(frame)
    new_frame = pd.concat(frames, axis=0)
    new_frame.set_index(['Measure', 'Climate Zone', 'Year'], inplace=True)
    return new_frame


def close_event():
    plt.close()


class Worker:
    def __init__(self, file_path, output_dir):
        self.wkbk = xw.Book(file_path)
        self.states = self.wkbk.sheets[STATE_SHEET]
        options = self.states.range('A4').api.Validation.Formula1[1:]
        self.states_list = [item.value for item in self.states.range(options) if item.value is not None]
        self.current_state = self.states.range('A4').value
        print(f'Current state: {self.current_state}')
        # states.range('A4').value = states_list[randint(0, len(states_list)-1)]

        self.climate_list = [item.value for item in self.states.range('F9:F60')]
        self.state_abbr_list = [item.value for item in self.states.range('B9:B60')]
        self.climate_dict = dict(zip(self.state_abbr_list, self.climate_list))
        self.state_df = {}
        self.dfs = {}
        self.output_dir = output_dir

    def make_dict_df(self, state: str) -> dict[str, pd.DataFrame]:
        """
        Create state level dictionary for proto buildings DataFrame.

        :param state: Name of current state
        :return state_df: dictionary of building data frames for each state.
        """
        dfs = {}
        clean_map = {}
        try:
            self.states.range('A4').value = state
            current_state_abbr = us.states.lookup(state).abbr
            current_state_climates = self.climate_dict[current_state_abbr]
        except (KeyError, AttributeError):
            if 'District' in state:
                current_state_abbr = 'DC'
                current_state_climates = self.climate_dict[current_state_abbr]
            else:
                return {}
        print(f'New state {state} has {current_state_climates} climate zones!')
        _range = f'I8:{data_map[current_state_climates]}160'
        for sheet_name in BUILDINGS:
            df = self.wkbk.sheets(sheet_name)
            _clean_map = [1, 2]
            for i, j in enumerate(df.range('A:A')[0:200].value):
                if 'VBA' in str(j):
                    break
                if str(j) == 'x':
                    _clean_map.append(i - 8)
            clean_map[sheet_name] = _clean_map
            print(state, sheet_name, clean_map[sheet_name])
            measure = df[f'B8:B160'].options(pd.DataFrame, header=1).value
            df2 = df[f'I8:{data_map[current_state_climates]}160'].options(pd.DataFrame).value  # HERE IS THE DATSAFRAME TO START WITH.
            df2['Measure'] = measure.index
            df2 = df2.reset_index(drop=False)
            df2 = df2.iloc[clean_map[sheet_name], :]
            df2 = create_frame(df2,
                               block_start_func=find_start_columns,
                               block_end_func=find_end_columns,
                               climate_zone_func=find_climate_zone_columns,
                               header_func=find_headers,
                               header_row_count=3)
            dfs[sheet_name] = df2
        return dfs

    def store_files(self):
        """Output state/building info to file"""
        # dir_path = Path(self.output_dir)
        # dir_path.mkdir(parents=True, exist_ok=True)
        for state_name, state_dict in self.state_df.items():
            for building_name, data in state_dict.items():
                data.to_csv(self.output_dir / f'{state_name}_{building_name}.csv')

    def replacement_cost_plot(self):
        """
        Create heat map for all replacement costs
        :param self:
        :return:
        """
        plotter = pd.DataFrame(index=list(list(self.state_df.values())[0].keys()))
        for state, state_dict in self.state_df.items():
            for building_name, data in state_dict.items():
                result = data['Total Replacement Cost'].astype(float).groupby(['Climate Zone', 'Year']).sum().groupby('Climate Zone').diff().groupby('Climate Zone').apply(lambda x: x.iloc[-1])
                # This is useful for bar plot but labels were too tight
                # result.index = result.index.map(lambda x: f'{state}, {building_name}, {x}')
                df = pd.DataFrame([result], index=[f'{building_name}'])
                df.columns = df.columns.map(lambda x: f'{state}: {x}')

                plotter.loc[df.index, df.columns] = df
        sns.heatmap(plotter, cmap='bwr', xticklabels=True, yticklabels=True)
        #threading.Timer(5.0, close_event).start()
        plt.show()

    def work_main(self):
        try:
            for state in self.states_list:
                try:
                    self.state_df[state] = self.make_dict_df(state)
                except Exception as ex:
                    print(f'Error for {state} -- {ex}!')
                    continue
        finally:
            self.wkbk.save()
            self.wkbk.close()


# #xlsm_file_path = 'C:/Users/lute362/OneDrive - PNNL/Alex-Robert-Matt-state/901-19_State_CE_Analysis_2021-05-26_EPv010_copy.xlsm'
# # Alex updated
# output_dir = 'C:/Users/vlac284/OneDrive - PNNL/Alex-Robert-Matt-state/working_versions_2024_08_05/hvac_data_CE/2019'
# xlsm_file_path = 'C:/Users/vlac284/OneDrive - PNNL/Alex-Robert-Matt-state/working_versions_2024_08_05/901-19_State_CE_Analysis_082024.xlsm'
#
# worker = Worker(xlsm_file_path, output_dir)
# worker.work_main()
# worker.store_files()
# worker.replacement_cost_plot()
