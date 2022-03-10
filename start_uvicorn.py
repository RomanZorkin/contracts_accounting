import configparser
import os
from pathlib import Path

import uvicorn

import api.app as ap

config = configparser.ConfigParser()
config.read(Path('config.ini'))

app = ap.app


if __name__ == "__main__":
    uvicorn.run(
        "api.app:app",
        host=config['uvicorn_rule']['host'],
        port=int(config['uvicorn_rule']['port']),
        log_level="info",
        reload=True
    )
    


"""
from uvicorn.reloaders.statreload import StatReload
from uvicorn.main import run, get_logger
reloader = StatReload(get_logger(run_config['log_level']))
reloader.run(run, {
    'app': app,
    'host': run_config['api_host'],
    'port': run_config['api_port'],
    'log_level': run_config['log_level'],
    'debug': 'true'
})
"""