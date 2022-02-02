"""Create SQLAlcheme database scheme"""
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml

config_file = Path('configuration/table.yaml')

with open(config_file) as fh:
    table_config = yaml.safe_load(fh)

table_list = list(table_config)

scheme_file = Path('api/sql_scheme.py')
shablon = Path('configuration/shablon.txt')
format_dict = {
    'text': 'Text',
    'integer': 'Integer',
    'date': 'Date',
    'numeric': 'Float',
    'boolean': 'Boolean',
}

def make_class(table_name: str) -> List[str]:
    return [
        '\n\n',
        f'class {table_name.capitalize()}(Base):',
        '\n',
        f"    __tablename__ = '{table_name}'",
        '\n\n',
    ]

def make_atributes(table_name: str) -> List[str]:
    columns_scheme = table_config[table_name]['columns']
    columns = list(columns_scheme)
    rule = []
    for column in columns:
        class_atribute = f'    {column.lower()} = Column({format_find(columns_scheme, column)})\n'
        rule.append(class_atribute)
    return rule

def format_find(columns_scheme: Dict[str, Any], column: str) -> str:
    format_part = columns_scheme[column]['format'].split(' ')
    links = columns_scheme[column]['links']
    link = ''
    if len(links) > 0:
        link = f", ForeignKey('{links[0]}.{column}')"
    if len(format_part) > 1:
        return f'{format_dict[format_part[0]]}{link}, primary_key=True'
    return f'{format_dict[format_part[0]]}{link}'


def creater() -> None:
    with open(scheme_file, 'w', encoding='utf-8') as sf:
        with open(shablon, 'r') as sh:
            start = sh.readlines()
        sf.writelines(start)
        for table_name in table_list:
            sf.writelines(make_class(table_name))
            sf.writelines(make_atributes(table_name))

creater()


"""
    pays_short = relationship('Payments_short', backref='deals')
    pays_full = relationship('Payments_full', backref='deals')
    reg_number_full = Column(Text, ForeignKey('deals.reg_number_full'))
"""