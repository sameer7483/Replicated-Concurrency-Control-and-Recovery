from enum import Enum
class TransactionStatus(Enum):
    READY = 1
    RUNNING = 2
    BLOCKED = 3
    ABORTED = 4
    COMMITTED = 5