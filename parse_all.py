import os
import re
from pathlib import Path
from dataclasses import dataclass, field

from parse_hvac import Worker as Hvac
from parse_cost import Worker as Cost

@dataclass
class Filehandler:
    input_dir: str
    output_dir: str
    input_files: list = field(default_factory=lambda: [])
    input_paths: list = field(default_factory=lambda: [])
    output_targets: list = field(default_factory=lambda: [])
    file_map: dict = field(default_factory=lambda: {})

    def __post_init__(self):
        self.input_files = os.listdir(self.input_dir)
        self.input_paths = [filepath for filepath in Path(self.input_dir).glob('**/*')]
        self.output_targets = ['/'.join([self.output_dir, y]) for y in ['20' + re.split(r'[-_]', f)[1] for f in self.input_files]]
        for input_path, output in zip(self.input_paths, self.output_targets):
            target = Path(output)
            target.mkdir(parents=True, exist_ok=True)
            self.file_map[input_path] = target


input_dir = 'inputs'
hvac_output_dir = 'hvac_data_CE'
cost_output_dir = 'cost_data_CE'
hvac_handler = Filehandler(input_dir, hvac_output_dir)
cost_handler = Filehandler(input_dir, cost_output_dir)
for i, o in hvac_handler.file_map.items():
    worker = Hvac(i, o)
    worker.work_main()
    worker.store_files()
#     # worker.replacement_cost_plot()

for i, o in cost_handler.file_map.items():
    worker = Cost(i, o)
    worker.work_main()
    worker.store_files()
