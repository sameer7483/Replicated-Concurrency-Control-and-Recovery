class Lock:
    '''Data model for a lock'''
    def __init__(self, lock_type, var_name, t_id=None):
        '''creates and initialises a new lock

        Parameters:
        lock_typ(LockType): type of the lock. i.e Read, Write
        var_name(string): name on the variable on which lock is applied
        t_id(string): name of the transaction applying this lock

        Returns:
        Lock: a new lock object initialized with the given values
        '''           
        self.lock_type = lock_type
        self.var_name = var_name
        self.t_id = t_id
