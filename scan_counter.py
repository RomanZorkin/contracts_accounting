"""Creates a file with a list of missing contract scans."""

import time
from pathlib import Path
from typing import Dict, List

import yaml

start_time = time.time()

config_file = Path('configuration/config.yaml')
with open(config_file) as fh:
    scan_config = yaml.load(fh, Loader=yaml.FullLoader)


class Counter(object):
    """Creates a file with a list of missing contract scans.

    When creating an object of the Counter class, the directory is
    checked in accordance with the specified year for the presence
    of contract scans in it. Contracts in the directory have a name
    in the form of "000.pdf", where "001.pdf" is the first contract,
    and "100.pdf" is the hundredth contract. The list of contracts
    located in the directory is compared with the list of contacts
    entered in the database. Missing scans of contracts and contract
    items are recorded in a CSV file

    Attributes:
        sql_sheme (Scheme): an object of the Scheme class in the sqlorm
            module with the specified table rules
        scan_dir (Path): the directory where the scans of contracts
            for a certain year are located
        csv_file (Path): the path to the csv file where the list of
            missing contract scans will be recorded
    """

    def __init__(self, sql_scheme, year):
        """Docstring _init_."""
        self.sql_scheme = sql_scheme
        self.scan_dir: Path = Path(scan_config['scans'], str(year))
        self.csv_file: Path = Path(self.scan_dir, 'scan.csv')

    def scan_list(self) -> List[int]:
        """Generate a list of PDF files names without extension.

        Generates a list of PDF files names without extension,
        located in the directory. The list is sorted from smaller to
        larger. The location of the directory is set in the
        configuration file.

        Returns:
            List[int]: list of files names without extension
        """
        return [
            int(scan.stem) for scan in sorted(
                Path(self.scan_dir).glob('*.pdf'),
            )
        ]

    def deals_list(self) -> List[int]:
        """Generate a list of contract numbers.

        Generates a list of contract numbers available in the database.
        The list includes all values of the column "reg_number"
        in table "deals". The list is sorted from smaller to larger.

        Returns:
            List[int]: list of contract numbers available in the database
        """
        return sorted(self.sql_scheme.read_all_column('reg_number'))

    def lost_list(self) -> List[int]:
        """Compare list of scans and list of contracts.

        The function compares the list of scanned copies of contracts
        in the directory and the list of contracts in the database.
        Returns a list of missing scanned copies.
        The list is sorted from smaller to larger.

        Returns:
            List[int]: list of contract numbers scanned copies
            of which are missing
        """
        return sorted(set(self.deals_list()) - set(self.scan_list()))

    def find_subject_all(self) -> Dict[int, str]:
        """Generate dict with contract number & subject.

        The function generates in the form of a dictionary the numbers
        of missing scans of contracts and the corresponding subject
        of the contract, which is obtained from the database

        Returns:
            Dict[int, str]: dict with contract number (key, int) and
            and its subject (value, str)
        """
        query_list = []
        for reg_number in self.lost_list():
            query = {
                'search_column': 'subject',
                'index': 'reg_number',
                'index_value': reg_number,
            }
            query_list.append(query)
        return self.sql_scheme.read_row_list(query_list)

    def make_report(self) -> None:
        """Make CSV file with missing contracts.

        The function creates a csv file with a list of numbers of missing
        scans of contracts and their items.
        The execution time of the function is passed also to the csv file.
        The output file is saved by default in the same directory where the
        target scans of contracts are stored.
        A message about the completion of the operation is sent
        to the dialog box.
        """
        with open(self.csv_file, 'w') as excel:
            excel.write(
                'Список отсутсвующих сканов государственных контрактов\n',
            )
            for number, subject in self.find_subject_all().items():
                excel.write(f' Дог № {number};{subject}\n')
            excel.write(
                f'--- {time.time() - start_time} время выполнения PYTHON скрипта ---',
            )
        print('Записан файл ', self.csv_file)
