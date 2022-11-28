class Variable:
    def __init__(self, name, val, lock):
        self.name = name
        self.val = val
        self.lock = lock 