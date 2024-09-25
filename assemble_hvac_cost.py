# -*- coding: utf-8 -*-
"""
lute362 2024/08/18
"""

import pandas as pd
from copy import copy
import csv
from pathlib import Path
import sys
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

BUILDINGS = [
    "HVAC Small Office Proto",
    "HVAC Large Office Proto",
    "HVAC Standalone Retail Proto",
    "HVAC Primary School Proto",
    "HVAC Small Hotel Proto",
    "HVAC Mid-rise Apartment Proto"
]


def concat_df(df_concat: list[pd.DataFrame], token: str) -> pd.DataFrame:
    """
    Join list of DataFrames, clean up column headers by stripping extra white space,
    and aggregate with and group by Measure, Climate Zone.
    :param df_concat: list of DataFrame objects to join
    :param token: 'Target' or 'Base' to apply to output file header columns for target and base code
    :return: Concatenated DataFrame from aggregate cost analysis
    """
    df = pd.concat(df_concat)
    df.rename(columns=lambda x: x.strip(), inplace=True)
    # Assumes that all Replacement Life values are the same for each group
    replacement_life = df.pop('Replacement Life').groupby(level=[0, 1]).last()
    # Sum HVAC costs by group
    df = df.groupby(level=[0, 1]).sum()
    df['Replacement Life'] = replacement_life
    # Remove extra white space from column headers
    rename = {item: token.strip() + ': ' + item.strip() for item in df.columns}
    df = df.rename(columns=rename)
    return df



def filter_df(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Filter Dataframe by desired year, set DataFrame multi-index to
    Measure, Climate Zone, and Year, and drop all NaN entries.
    :param df: DataFrame to filter by year and set_index
    :param year: code year to apply filter.
    :return: Filtered dataframe
    """
    df = df[df.Year == year]
    df.set_index(['Measure', 'Climate Zone', 'Year'], inplace=True)
    df = df.fillna(0)
    return df


def create_cost_map(file_name: str) -> dict[str, dict[str, list[int]]]:
    """
    Create mapper from input file.
    :param file_name: file with base/target mapping
    :return: mapper to target and base code for each state used to process and create cost analysis
    """
    mapper = {}
    with open(file_name, 'r') as csvfile:
        datareader = csv.DictReader(csvfile)
        for obj in datareader:
            try:
                target = [int(item) for item in obj['target'].split(';')]
                base = [int(item) for item in obj['base'].split(';')]
                mapper[obj['state']] = {
                    'target': target,
                    'base': base
                }
            except KeyError as ex:
                print(f'File is on formatted correctly: {file_name} -- {ex}')
                sys.exit()
            except ValueError as ex:
                print(f'target or base year is not convertible to numeric value for {obj} -- {ex}')
                continue
    return mapper


class Worker:
    def __init__(self, output_dir):
        self.output_dir = output_dir

    def store_files(self, df: pd.DataFrame, filename: str):
        """
        Output state/building info to file
        :param df: aggregate DF to output for HVAC cost.
        :param filename: file name based on state and building.
        :return:
        """
        cost_path = Path(self.output_dir)
        cost_path.mkdir(parents=True, exist_ok=True)
        # for state_name, state_df in self.state_df.items():
        df.to_csv(cost_path / f'{filename}.csv')

    @staticmethod
    def work_main(filename: str, base: int, target: int) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Main Function for constructing baseline and target HVAC cost aggregation.
        :param filename: HVAC input file based on state, building type, year.
        :param base: year for baseline.
        :param target: year for target.
        :return:
        """
        df = pd.read_csv(filename)
        base_df = filter_df(copy(df), base)
        target_df = filter_df(copy(df), target)
        return base_df, target_df


def main():
    ######################################################################
    # Configuration of script.
    input_directory = 'hvac_data_CE'
    master_file = 'inputs/current_vs_target_master2.csv'
    output_directory = 'hvac_assembled_cost'
    ######################################################################

    mapper = create_cost_map(master_file)
    worker = Worker(output_directory)
    aggregate_df = []

    def process_building_data(state, building, info):
        df_base_years, df_target_years = [], []
        file_name = f'{state}_{building}'
        for target_year, base_year in zip(info['target'], info['base']):
            input_file = f'{input_directory}/{target_year}/{file_name}.csv'
            print(f'Process file {input_file}')
            base_data, target_data = worker.work_main(input_file, base_year, target_year)
            df_base_years.append(base_data)
            df_target_years.append(target_data)
        df_base = concat_df(df_base_years, 'Base')
        df_target = concat_df(df_target_years, 'Target')
        return df_base.join(df_target), file_name

    def update_dataframe_index(out_df, state, building):
        old_index = out_df.index.to_frame()
        old_index.insert(0, 'State', state)
        old_index.insert(1, 'Building', building)
        out_df.index = pd.MultiIndex.from_frame(old_index)
        return out_df

    for state, info in mapper.items():
        for building in BUILDINGS:
            try:
                processed_data, file_name = process_building_data(state, building, info)
                worker.store_files(processed_data, file_name)
                updated_df = update_dataframe_index(processed_data, state, building)
                aggregate_df.append(updated_df)
            except Exception as ex:
                print(f'Error for state: {state} --- building: {building} -- {ex}!')
                continue

    final_aggregate_df = pd.concat(aggregate_df)
    worker.store_files(final_aggregate_df, 'aggregate_hvac')


if __name__ == '__main__':
    main()
