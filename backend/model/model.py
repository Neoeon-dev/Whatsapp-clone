from enum import Enum, auto

class ErrorCode(Enum):
    USER_ALREADY_EXISTS = auto()
    INVALID_USER = auto()
    USER_DOESNT_EXIST = auto()
    WRONG_PASSWORD = auto()
    SAFE = auto()