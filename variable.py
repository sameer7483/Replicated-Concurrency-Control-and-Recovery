class Variable:
    def __init__(self, name, val, lock, replicated = False):
        self.name = name
        self.val = val
        self.lock = lock
        self.commited_value = val
        self.commited_time = 0
        self.replicated = replicated
        self.readable = True
        self.version_history = {0:val} #dict of time and val