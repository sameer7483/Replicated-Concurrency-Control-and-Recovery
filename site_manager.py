from status import Status
class Site:
    def __init__(self,name, status, vars):
        self.name = name
        self.status = status
        self.variables = vars #dict of var name and var obj
        self.lock_table = dict() # dict of var name and lock obj
    
    def fail(self):
        self.status = Status.FAILED
        self.lock_table.clear()
    
    def recover(self):
        pass

class SiteManager:
    def __init__(self, sites):
        self.sites = sites
