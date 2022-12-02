class Transaction:
    '''Data Model for the Transaction'''
    def __init__(self, id, status, start_time, read_only = False):
        '''creates and initialises a new transaction

        Parameters:
        id(string): id of the transaction
        status(TransactionStatus): current status of the Transaction
        start_time(int): time at which the transaction started
        read_only(bool): True if transaction is read-only otherwise False

        Returns:
        Transaction: a new transaction object initialized with the given values
        '''        
        self.id = id
        self.status = status
        self.start_time = start_time
        self.read_only = read_only
        self.sites_accessed = set()
