class Lock:
    def __init__(self, lock_type, var_name, t_id=None):
        self.lock_type = lock_type
        self.var_name = var_name
        self.t_id = t_id
