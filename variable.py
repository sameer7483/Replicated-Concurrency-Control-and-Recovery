class Variable:
    '''Data Model for the Variable'''
    def __init__(self, name, val, lock, replicated = False):
        '''creates and initialises a new variable

        Parameters:
        name(string): name of the variable
        val(int): value of the variable
        lock(list): list of locks on the variable
        replicated(bool): True if variable is replicated across multiple sites

        Returns:
        Variable: a new variable object initialized with the given values
        '''
        self.name = name
        self.val = val
        self.lock = lock
        self.commited_value = val
        self.commited_time = 0
        self.replicated = replicated
        self.readable = True
        self.version_history = {0:val} #dict of time and val