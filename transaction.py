class Transaction:
    def __init__(self, name, status, start_time, read_only = False):
        self.name = name
        self.status = status
        self.start_time = start_time
        self.read_only = read_only
        self.sites_accessed = set()
