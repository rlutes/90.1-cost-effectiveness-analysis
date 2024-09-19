# -*- coding: utf-8 -*-
"""
lute362 2024/08/18

climate_zones = ['1A', '1B', '2A', '2B', '3A', '3B', '4A', '4B', '5A', '5B', '6A', '6B', '7', '8']
"""

import pandas as pd
from copy import copy
import csv
from pathlib import Path
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def filter_df(df: pd.DataFrame, state: str, _year) -> pd.DataFrame:
    """
    Filter out HVAC and Total cost.  Coerce Cost to float if strings are present.
    :param df: Incremental cost for lighting and envelope
    :param state: Name of state for inclusion in data
    :param _year: Year of code for inclusion in data
    :return: filtered data
    """
    df = df[df.DeviceType != 'HVAC']
    df = df[df.DeviceType != 'Total']
    # df = df[df.Cost != 0]
    df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce")
    df['State'] = state
    df['CodeYear'] = _year
    df = df.fillna(0)
    return df


def create_cost_map(filename: str) -> dict:
    """
    Create mapper from input file.
    :param filename: file with base/target mapping
    :return:
    """
    mapper = {}
    with open(filename, 'r') as csvfile:
        datareader = csv.DictReader(csvfile)
        for obj in datareader:
            if ';' in obj['target']:
                target = [int(item) for item in obj['target'].split(';')]

            else:
                target = [int(obj['target'])]
            mapper[obj['state']] = target
    return mapper

def store_files(df: pd.DataFrame, output_dir: str, filename: str):
    """
    Output state/building info to file
    :param df: aggregate DF to output for HVAC cost.
    :param output_dir: path for output_directory
    :param filename: file name based on state and building.
    :return:
    """
    cost_path = Path(output_dir)
    cost_path.mkdir(parents=True, exist_ok=True)
    # for state_name, state_df in self.state_df.items():
    df.to_csv(cost_path / f'{filename}.csv')


def assemble(filename: str, state: str, yr: int) -> pd.DataFrame:
    """
    :param filename: HVAC input file based on state, building type, year.
    :param state: state to analyze
    :param yr: year for target.
    :return: filtered DataFrame
    """
    df = pd.read_csv(filename)
    target_df = filter_df(copy(df), state, yr)
    return target_df


if __name__ == '__main__':
    ######################################################################
    # Configuration of script.

    # Input directory
    directory = 'cost_data_CE'

    # File that specifies the base, target years for analysis
    master_file = 'inputs/current_vs_target_master_exclude_CE_2010.csv'

    # Output directory, will create if it does not exist
    output_dir = 'light_envelope_assembled_cost'

    # Output file name, will have extension .csv
    output_file = 'light_envelope_cost'
    ######################################################################
    mapper = create_cost_map(master_file)
    final_df = []
    for state, info in mapper.items():
        try:
            df_t = []
            for _year in info:
                input_file = '/'.join([directory, str(_year), state + '.csv'])
                print(f'Process file {input_file}')
                t = assemble(input_file, state, _year)
                df_t.append(t)
            df_target = pd.concat(df_t)
            final_df.append(df_target)
        except Exception as ex:
            print(f'Problem for {state} -- {ex}')

    out_df = pd.concat(final_df)
    out_df = pd.pivot_table(out_df, index=['State', 'Building', 'CodeYear', 'DeviceType', 'Year'], columns='ClimateZone', values='Cost')
    store_files(out_df, output_dir, output_file)

