from status import Status
from lock_type import LockType
from collections import defaultdict
from variable import Variable
from lock import Lock
import bisect
class Site:
    '''class that describes a Site'''
    def __init__(self,name, status, vars):
        '''creates and initialises a new Site

        Parameters:
            self(Site): instance of the class
            name(string): name of the site
            status(Status): status of the Site i.e AVAILABLE, FAILED
            vars(dict): dictionary of variable name and object
        Returns:
            Site: a new Site object initialized with the given values
        '''  
        self.name = name
        self.status = status
        self.variables = vars #dict of var name and var obj
        self.lock_table = defaultdict(list) # dict of var name and list of locks

    def can_acquire_read_lock(self, t_id, var_name):
        '''Method to test if a read lock can be acquired by transaction t_id on variable var_name

        Parameters:
            self(Site): instance of the class
            t_id(string): name of the transaction
            var_name(string): name of the var_name
        Returns:
            Boolean: return True if t_id can acquire Read lock otherwise False
        '''          
        for lock in self.lock_table[var_name]:
            if lock.t_id != t_id and lock.lock_type == LockType.WRITE: 
                return False
        return True

    def acquire_read_lock(self, t_id, var_name):
        '''Method to acquire read lock by transaction t_id on variable var_name

        Parameters:
            self(Site): instance of the class
            t_id(string): name of the transaction
            var_name(string): name of the var_name
        SideEffect:
            Add the lock to the locktable of the current site.
        '''          
        for lock in self.lock_table[var_name]:
            if lock.t_id == t_id:
                return
        self.lock_table[var_name].append(Lock(LockType.READ, var_name, t_id))

    def can_acquire_write_lock(self, t_id, var_name):
        '''Method to test if a write lock can be acquired by transaction t_id on variable var_name

        Parameters:
            self(Site): instance of the class
            t_id(string): name of the transaction
            var_name(string): name of the var_name
        Returns:
            Boolean: return True if t_id can acquire Write lock otherwise False
        '''          
        for lock in self.lock_table[var_name]:
            if lock.lock_type != LockType.NO_LOCK and lock.t_id != t_id:
                return False
        return True

    def acquire_write_lock(self, t_id, var_name):
        '''Method to acquire write lock by transaction t_id on variable var_name

        Parameters:
            self(Site): instance of the class
            t_id(string): name of the transaction
            var_name(string): name of the var_name
        SideEffect:
            if lock doesn't exist then add the lock to the locktable of the current site otherwise
            promotes the lock to a Write lock.
        '''          
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
        '''Method to release locks by transaction t_id on variable var_name

        Parameters:
            self(Site): instance of the class
            t_id(string): name of the transaction
            var_name(string): name of the var_name
        SideEffect:
            update the lock table of the current site by removing all the locks held by t_id on var_name
        '''          
        cpy_list = []
        for lock in self.lock_table[var_name]:
            if lock.t_id != t_id:
                cpy_list.append(lock)
        self.lock_table[var_name] = cpy_list

    def fail(self):
        '''Method to fail a Site

        Parameters:
            self(Site): Object of Site class
        SideEffect:
            update the status of the Site to Failed and clear all the locks held at the Site
        '''          
        self.status = Status.FAILED
        for key, locks in self.lock_table.items():
            locks.clear()
        #make all replicated var as non-readable:
        for var in self.variables.values():
            if var.replicated:
                var.readable = False
    
    def recover(self):
        '''Method to recover a failed Site

        Parameters:
            self(Site): Object of Site class
        SideEffect:
            update the status of a failed Site to Available
        '''          
        self.status = Status.AVAILABLE
    
    def can_read(self, t_id, var_name):
        '''Method to test if the given transaction can read the given variable at this Site

        Parameters:
            self(Site): instance of the class
            t_id(string): name of the transaction
            var_name(string): name of the var_name
        Returns:
            Boolean: return True if t_id can read var_name at this Site
        '''         
        if self.status != Status.AVAILABLE:
            return False
        if var_name not in self.variables:
            return False
        if self.variables[var_name].readable == False:
            return False
        return self.can_acquire_read_lock(t_id, var_name)

    def can_read_read_only(self, var_name):
        '''Method to test if the given variable can be read at this Site

        Parameters:
            self(Site): instance of the class
            var_name(string): name of the var_name
        Returns:
            Boolean: return True if var_nam can be read at this Site otherwise False
        '''        
        if self.status != Status.AVAILABLE:
            return False
        if var_name not in self.variables:
            return False
        if self.variables[var_name].readable == False:
            return False 
        return True          

    def can_write(self, t_id, var_name):
        '''Method to test if the given transaction can write the given variable at this Site

        Parameters:
            self(Site): instance of the class
            t_id(string): name of the transaction
            var_name(string): name of the var_name
        Returns:
            Boolean: return True if t_id can write to var_name at this Site
        '''           
        if self.status != Status.AVAILABLE:
            return False
        if var_name not in self.variables:
            return False
        return True  

    def get_locking_transaction(self, var):
        '''Method to get all the transactions that hold lock on variable var at this Site.

        Parameters:
            self(Site): instance of the class
            var(string): name of the variable
        Returns:
            set: a set of transactions that has lock on the given variable
        '''            
        t_ids = set()
        for lock in self.lock_table[var]:
            t_ids.add(lock.t_id)
        return t_ids
    
    def print_site_status(self):
        '''Method to print the status of this Site.

        Parameters:
            self(Site): instance of the class.
        SideEffect:
            prints the name and status of the Site
        '''           
        print(f'{self.name}: {self.status}')

    def get_locking_transaction_on_site(self):
        '''Method to get all the transactions that hold lock on any variable at this Site.

        Parameters:
            self(Site): instance of the class.
        Returns:
            set: a set of transactions that has lock on any variable at this Site.
        '''           
        t_ids = set()
        for locks in self.lock_table.values():
            for lock in locks:
                t_ids.update(lock.t_id)
        return t_ids


class SiteManager:
    '''class that manages all the sites and abstracts the underlying distribution of the Site'''
    def __init__(self, num_site, num_var):
        '''creates and initialises a new Site Manager

        Parameters:
            self(SiteManager): instance of the class.
        Returns:
        Instruction: a new SiteManager object initialized with give number of Sites and Variables.
        '''           
        self.num_site = num_site
        self.num_var = num_var
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
        '''Reads the variable var for the given transaction

        Parameters:
            self(SiteManager): instance of the class.
            transaction(Transaction): instance of transaction
            var(string): name of the variable whose value is being accessed
        Returns:
        tuple: a tuple of variable values and site from which it is read
        '''           
        t_id = transaction.id
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
        '''Writes the variable var with the value val for the given transaction t_id

        Parameters:
            self(SiteManager): instance of the class.
            t_id(string): id of the transaction
            var(string): name of the variable whose value is being accessed
            val(int): value to which the variable var needs to be written
        Returns:
        list: a list of all sites to which var is written
        '''          
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
        '''commit the transaction with id t_id

        Parameters:
            self(SiteManager): instance of the class.
            t_id(string): id of the transaction
            var(string): name of the variable whose value is being accessed
            val(int): value to which the variable var needs to be written
        Returns:
        list: a list of all sites to which var is written
        '''            
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
        '''Prints the site name and all current committed state of each variable that it contains

        Parameters:
            self(SiteManager): instance of the class.        
        '''           
        for name, site in self.sites.items():
            var_vals = [var.name +':'+str(var.commited_value) for var in site.variables.values()]
            string_var_vals = ','.join(var_vals)
            print(f'{name}: {string_var_vals}')

    def print_lock_table(self, site_name):
        '''Prints the lock table of the give site.

        Parameters:
            self(SiteManager): instance of the class. 
            site_name(string): name of the site.      
        '''           
        for var, locks in self.sites[site_name].lock_table.items():
            locking_trans = ', '.join([lock.t_id for lock in locks])
            print(f'{var}: {locking_trans}')

    def fail(self, site_name):
        '''Fails the given site.

        Parameters:
            self(SiteManager): instance of the class. 
            site_name(string): name of the site.      
        '''         
        self.sites[site_name].fail()
        print(f'site: {site_name} failed')
    
    def abort(self, site_name, t_id, time):
        '''Aborts the transaction t_id

        Parameters:
            self(SiteManager): instance of the class. 
            site_name(string): name of the site.   
            t_id(string): transaction id
            time: time of abort
        SideEffect:
            Removes all the locks held by transaction t_id at site site_name
        '''         
        for var in self.sites[site_name].variables.values():
            if t_id in self.sites[site_name].get_locking_transaction(var.name):
                var.val = var.commited_value
        for locks in self.sites[site_name].lock_table.values():
            locks[:] = [lock for lock in locks if lock.t_id != t_id]

    def recover(self, site_name):
        '''Recovers the given failed site

        Parameters:
            self(SiteManager): instance of the class. 
            site_name(string): name of the site.   
        '''          
        self.sites[site_name].recover()
        print(f'site: {site_name} recovers')
    
    def get_locking_transaction(self, var):
        '''Method to get all the transactions that hold lock on variable var.

        Parameters:
            self(SiteManager): instance of the class
            var(string): name of the variable
        Returns:
            set: a set of transactions that has lock on the given variable
        '''           
        t_ids = set()
        for site in self.sites.values():
            t_ids.update(site.get_locking_transaction(var))
        return t_ids

    def print_all_site_status(self):
        '''Method to print the status of all the Sites.

        Parameters:
            self(SiteManager): instance of the class.
        SideEffect:
            prints the name and status of each Site
        '''                
        for site in self.sites.values():
            site.print_site_status()

    def get_locking_transaction_on_site(self, site_name):
        '''Method to get all the transactions that hold lock on any variable at this Site.

        Parameters:
            self(SiteManager): instance of the class.
            site_name(string): name of the site
        Returns:
            set: a set of transactions that has lock on any variable at this Site.
        '''   
        return self.sites[site_name].get_locking_transaction_on_site()


