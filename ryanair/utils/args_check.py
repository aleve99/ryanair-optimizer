from typing import Tuple, Iterable
from pathlib import Path
from argparse import ArgumentTypeError

def check_paths(paths_exts: Iterable[Tuple[Path, str]]):
    """Check file and correct extension or raise :class:`FileNotFoundError`"""

    for path, extension in paths_exts:
        if not path.is_file() or path.name.split('.')[-1] != extension:
            raise FileNotFoundError(
                f"{path.absolute()} is not a .{extension} file or doesn't exist"
            )
        
def check_positive(value):
    ivalue = int(value)
    if ivalue < 0:
        raise ArgumentTypeError(f"{value} is an invalid positive int value")
    
    return ivalue
