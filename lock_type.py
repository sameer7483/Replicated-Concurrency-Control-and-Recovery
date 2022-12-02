from enum import Enum
class LockType(Enum):
    '''Enum class depicting the types of Lock that a transaction can acquire on variables'''
    NO_LOCK = 1
    READ = 2
    WRITE = 3