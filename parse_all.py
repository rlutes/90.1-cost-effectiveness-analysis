import os
import re
from pathlib import Path
from dataclasses import dataclass, field

from parse_hvac import Worker as Hvac
from parse_cost import Worker as Cost

@dataclass
class Filehandler:
    """
    Reads input files and creates file map.  Handles creation of output directory and creation
    of output file path map.
    """
    input_dir: str
    output_dir: str
    input_files: list = field(default_factory=lambda: [])
    input_paths: list = field(default_factory=lambda: [])
    output_targets: list = field(default_factory=lambda: [])
    file_map: dict = field(default_factory=lambda: {})

    def __post_init__(self):
        self.input_files = os.listdir(self.input_dir)
        self.input_paths = [filepath for filepath in Path(self.input_dir).glob('**/*.xlsm')]
        self.output_targets = ['/'.join([self.output_dir, y]) for y in ['20' + re.split(r'[-_]', f)[1] for f in self.input_files]]
        for input_path, output in zip(self.input_paths, self.output_targets):
            target = Path(output)
            target.mkdir(parents=True, exist_ok=True)
            self.file_map[input_path] = target


if __name__ == '__main__':

    input_dir = 'inputs'
    hvac_output_dir = 'hvac_data_CE'
    cost_output_dir = 'cost_data_CE'
    hvac_handler = Filehandler(input_dir, hvac_output_dir)
    cost_handler = Filehandler(input_dir, cost_output_dir)
    # Run loops for the parser methods.  Creates all needed input files for the HVAC cost and
    # lighting/envelope cost extraction.
    for i, o in hvac_handler.file_map.items():
        try:
            print(f'Processing file {i} for HVAC costs store results in {o}')
            worker = Hvac(i, o)
            worker.work_main()
            worker.store_files()
            # worker.replacement_cost_plot()
        except Exception as ex:
            print(f'Problem parsing input file: {i} -- {ex}')
            continue

    for i, o in cost_handler.file_map.items():
        try:
            print(f'Processing file {i} for lighting and envelope costs store results in {o}')
            worker = Cost(i, o)
            worker.work_main()
            worker.store_files()
        except Exception as ex:
            print(f'Problem parsing input file: {i} -- {ex}')
            continue

