import os
import re
from pathlib import Path
from dataclasses import dataclass, field

from parse_hvac import Worker as Hvac
from parse_cost import Worker as Cost

######################################################################
# Configure script
HVAC_OUTPUT_DIR = 'hvac_data_CE'
COST_OUTPUT_DIR = 'cost_data_CE'
INPUT_DIR = 'inputs'
######################################################################

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

def process_files(filehandler, worker_class, description):
    for input_file, output_file in filehandler.file_map.items():
        try:
            print(f'Processing file {input_file} for {description} store results in {output_file}')
            worker = worker_class(input_file, output_file)
            worker.work_main()
            worker.store_files()
        except Exception as ex:
            print(f'Problem parsing input file: {input_file} -- {ex}')
            continue


if __name__ == '__main__':
    hvac_handler = Filehandler(INPUT_DIR, HVAC_OUTPUT_DIR)
    cost_handler = Filehandler(INPUT_DIR, COST_OUTPUT_DIR)

    process_files(hvac_handler, Hvac, "HVAC costs")
    process_files(cost_handler, Cost, "lighting and envelope costs")


