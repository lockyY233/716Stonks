from enum import Enum


class Rank(str, Enum):
    PRIVATE = "PRIVATE"
    CORPORAL = "CORPORAL"
    SERGEANT = "SERGEANT"
    LIEUTENANT = "LIEUTENANT"
    CAPTAIN = "CAPTAIN"
    MAJOR = "MAJOR"
    COLONEL = "COLONEL"
    GENERAL = "GENERAL"
