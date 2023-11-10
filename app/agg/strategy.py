from enum import Enum


class Strategy(Enum):
    IGNORE_POD_PHASES = 0
    CONSIDER_POD_PHASES = 1
    CONSIDER_ONLY_RUNNING_PODS = 2
