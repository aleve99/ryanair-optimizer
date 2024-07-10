import sys
from pathlib import Path
from typing import Iterable, List, Dict


if  sys.version_info.major < 3 \
    or (sys.version_info.major >= 3 and sys.version_info.minor < 11):

    import tomli as tomllib
else:
    import tomllib

    
def parse_toml(path: Path) -> dict:
    with open(path, "rb") as file:
        toml = tomllib.load(file)
    return toml

def parse_proxies(
          path: Path
    ) -> List[Dict[str, str]]:

    proxies = []
    with open(path, 'r') as file:
        for line in file.readlines():
            proxies.append(dict(
                http=line.strip(), https=line.strip()
            ))
            
    return proxies