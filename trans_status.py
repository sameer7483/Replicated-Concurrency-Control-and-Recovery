from enum import Enum
class TransactionStatus(Enum):
    '''Enum class depicting the multiple states of a Transaction'''
    READY = 1
    RUNNING = 2
    BLOCKED = 3
    ABORTED = 4
    COMMITTED = 5