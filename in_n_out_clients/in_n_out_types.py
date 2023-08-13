from enum import Enum
from typing import Dict


class ConflictResolutionStrategy(str, Enum):
    FAIL = "fail"
    REPLACE = "replace"
    IGNORE = "ignore"
    APPEND = "append"


APIResponse = Dict[str, int | str | list]
