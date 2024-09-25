# -*- coding: utf-8 -*-
"""
lute362 2024/08/18

climate_zones = ['1A', '1B', '2A', '2B', '3A', '3B', '4A', '4B', '5A', '5B', '6A', '6B', '7', '8']
"""

import pandas as pd
from copy import copy
import csv
from pathlib import Path
import os
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


def create_cost_map(file_path: str) -> dict[str, list[int]]:
    """
    Create a mapping from the input file.
    :param file_path: file with state/target mapping
    :return: Dictionary mapping state to target costs
    """

    def parse_target(target_str: str) -> list:
        """Parse the target string into a list of integers."""
        if ';' in target_str:
            return [int(item) for item in target_str.split(';')]
        return [int(target_str)]

    cost_mapper = {}
    with open(file_path, 'r') as csv_file:
        data_reader = csv.DictReader(csv_file)
        for record in data_reader:
            state = record['state']
            target = parse_target(record['target'])
            cost_mapper[state] = target

    return cost_mapper

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


def main():
    """
    Configures script parameters and executes the main processing steps, including creating a cost map and processing state data. Catches and prints exceptions that occur during execution.
    :return: None
    """
    # Configuration of script.
    input_directory = 'cost_data_CE'
    master_file_path = 'inputs/current_vs_target_master_exclude_CE_2010.csv'
    output_directory = 'light_envelope_assembled_cost'
    output_filename = 'light_envelope_cost'

    try:
        mapper = create_cost_map(master_file_path)
        process_states(mapper, input_directory, output_directory, output_filename)
    except Exception as ex:
        print(f'An exception occured when constructing year mapping: {ex}')


def process_states(mapper, input_directory, output_directory, output_filename):
    final_dataframes = []
    for state, years in mapper.items():
        try:
            yearly_dataframes = [
                assemble(os.path.join(input_directory, str(year), f'{state}.csv'), state, year) for
                                 year in years]
            state_dataframe = pd.concat(yearly_dataframes)
            final_dataframes.append(state_dataframe)
        except Exception as e:
            print(f'Problem for {state} -- {e}')
            continue

    combined_dataframe = pd.concat(final_dataframes)
    pivoted_dataframe = pd.pivot_table(combined_dataframe,
                                       index=['State', 'Building', 'CodeYear', 'DeviceType', 'Year'],
                                       columns='ClimateZone',
                                       values='Cost')
    store_files(pivoted_dataframe, output_directory, output_filename)


if __name__ == '__main__':
    main()

