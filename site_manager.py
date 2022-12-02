from status import Status
from lock_type import LockType
from collections import defaultdict
from variable import Variable
from lock import Lock
import bisect
class Site:
    def __init__(self,name, status, vars):
        self.name = name
        self.status = status
        self.variables = vars #dict of var name and var obj
        self.lock_table = defaultdict(list) # dict of var name and list of locks

    def can_acquire_read_lock(self, t_id, var_name):
        for lock in self.lock_table[var_name]:
            if lock.t_id != t_id and lock.lock_type == LockType.WRITE: 
                return False
        return True

    def acquire_read_lock(self, t_id, var_name):
        for lock in self.lock_table[var_name]:
            if lock.t_id == t_id:
                return
        self.lock_table[var_name].append(Lock(LockType.READ, var_name, t_id))

    def can_acquire_write_lock(self, t_id, var_name):
        for lock in self.lock_table[var_name]:
            if lock.lock_type != LockType.NO_LOCK and lock.t_id != t_id:
                return False
        return True

    def acquire_write_lock(self, t_id, var_name):
        found = False
        curr_lock = None
        for lock in self.lock_table[var_name]:
            if lock.t_id == t_id:
                curr_lock = lock
                found = True
                break
        if not found:
            self.lock_table[var_name].append(Lock(LockType.WRITE, var_name, t_id))
        else:
            curr_lock.lock_type = LockType.WRITE # promote lock to write lock


    def release_locks(self,t_id, var_name):
        cpy_list = []
        for lock in self.lock_table[var_name]:
            if lock.t_id != t_id:
                cpy_list.append(lock)
        self.lock_table[var_name] = cpy_list

    def fail(self):
        self.status = Status.FAILED
        for key, locks in self.lock_table.items():
            locks.clear()
        #make all replicated var as non-readable:
        for var in self.variables.values():
            if var.replicated:
                var.readable = False
    
    def recover(self):
        self.status = Status.AVAILABLE
    
    def can_read(self, t_id, var_name):
        if self.status != Status.AVAILABLE:
            return False
        if var_name not in self.variables:
            return False
        if self.variables[var_name].readable == False:
            return False
        return self.can_acquire_read_lock(t_id, var_name)

    def can_read_read_only(self, var_name):
        if self.status != Status.AVAILABLE:
            return False
        if var_name not in self.variables:
            return False
        if self.variables[var_name].readable == False:
            return False 
        return True          

    def can_write(self, t_id, var_name):
        if self.status != Status.AVAILABLE:
            return False
        if var_name not in self.variables:
            return False
        return True  

    def get_locking_transaction(self, var):
        t_ids = set()
        for lock in self.lock_table[var]:
            t_ids.add(lock.t_id)
        return t_ids
    
    def print_site_status(self):
        print(f'{self.name}: {self.status}')

    def get_locking_transaction_on_site(self):
        t_ids = set()
        for locks in self.lock_table.values():
            for lock in locks:
                t_ids.update(lock.t_id)
        return t_ids


class SiteManager:
    def __init__(self):
        num_site = 10
        num_var = 20
        sites = defaultdict(Site)
        for i in range(1, num_site+1):
            variables = defaultdict(Variable)
            for j in range(1, num_var+1):
                if j % 2 == 0:
                    var = Variable('x'+str(j), 10*j, Lock(LockType.NO_LOCK, 'x'+str(j)), True)
                    variables['x'+str(j)] = var   
                elif j % 2 != 0 and (1+ j%10) == i:
                    var = Variable('x'+str(j), 10*j, Lock(LockType.NO_LOCK, 'x'+str(j)))
                    variables['x'+str(j)] = var                         
            s = Site(str(i),Status.AVAILABLE, variables)
            sites[str(i)] = s        
        self.sites = sites

    def read(self, transaction, var):
        t_id = transaction.name
        read_only = transaction.read_only
        start_time = transaction.start_time
        if read_only:
            for name, site in self.sites.items():
                if site.can_read_read_only(var):
                    time_list = list(site.variables[var].version_history.keys())
                    idx = bisect.bisect_left(time_list, start_time)
                    return (site.variables[var].version_history[time_list[idx-1]], site.name)  
        for name, site in self.sites.items():
            if site.can_read(t_id, var):
                site.acquire_read_lock(t_id, var)
                return (site.variables[var].val, site.name)
        return (None, None) #cannot read bcoz of conflict
        
    def write(self, t_id, var, val):
        can_write_all = True
        sites_written = []
        for name, site in self.sites.items():
            if site.can_write(t_id, var):
                can_write_all = can_write_all and site.can_acquire_write_lock(t_id, var)
        if can_write_all:
            for name, site in self.sites.items():
                if site.can_write(t_id, var):
                    site.acquire_write_lock(t_id,var)
                    site.variables[var].val = val
                    sites_written.append(name)
        return sites_written
 
    def commit(self, site_name, t_id, time):
        if self.sites[site_name].status != Status.AVAILABLE:
            return
        for locks in self.sites[site_name].lock_table.values():
            for lock in locks:
                if lock.t_id == t_id and lock.lock_type == LockType.WRITE:
                    #commit the values
                    self.sites[site_name].variables[lock.var_name].commited_value = self.sites[site_name].variables[lock.var_name].val
                    self.sites[site_name].variables[lock.var_name].commited_time = time
                    self.sites[site_name].variables[lock.var_name].readable = True
                    self.sites[site_name].variables[lock.var_name].version_history[time] = self.sites[site_name].variables[lock.var_name].val
            locks[:] = [lock for lock in locks if lock.t_id != t_id]
    
    def dump(self):
        for name, site in self.sites.items():
            var_vals = [var.name +':'+str(var.commited_value) for var in site.variables.values()]
            string_var_vals = ','.join(var_vals)
            print(f'{name}: {string_var_vals}')

    def print_lock_table(self, site_name):
        for var, locks in self.sites[site_name].lock_table.items():
            locking_trans = ', '.join([lock.t_id for lock in locks])
            print(f'{var}: {locking_trans}')

    def fail(self, site):
        self.sites[site].fail()
        print(f'site: {site} failed')
    
    def abort(self, site_name, t_id, time):
        for locks in self.sites[site_name].lock_table.values():
            locks[:] = [lock for lock in locks if lock.t_id != t_id]
        for var in self.sites[site_name].variables.values():
            if t_id in self.sites[site_name].get_locking_transaction(var.name):
                var.val = var.commited_value

    def recover(self, site):
        self.sites[site].recover()
    
    def get_locking_transaction(self, var):
        t_ids = set()
        for site in self.sites.values():
            t_ids.update(site.get_locking_transaction(var))
        return t_ids

    def print_all_site_status(self):
        for site in self.sites.values():
            site.print_site_status()

    def get_locking_transaction_on_site(self, site):
        return self.sites[site].get_locking_transaction_on_site()


