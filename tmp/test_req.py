import yaml
from pathlib import Path


config_file = Path('configuration/table.yaml')

with open(config_file) as fh:
    table_config = yaml.safe_load(fh)


for table in table_config.values():
    test = table['columns']
    print(table['table_name'])
    for column, rule in test.items():
        #print(column)
        #print(rule) 
        if rule['links']:                       
            print(rule['links'])
            if len(rule['links']) > 0:
                for link in rule['links']:
                    print(
                        'связь с таблицей',
                        table_config[link]['columns'][column]
                    )


