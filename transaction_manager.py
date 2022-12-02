from transaction import Transaction
from trans_status import TransactionStatus
from collections import deque, defaultdict
from instruction import Instruction
from instruction_type import InstructionType
from site_manager import SiteManager


class TransactionManager:
    '''class that manages all the transactions'''
    def __init__(self):
        '''creates and initialises a new Transaction Manager
        Parameters:
            self(TransactionManager): instance of the class
        Returns:
            TransactionManager: a new TransactionManager object initialized with the given values
        '''          
        self.transaction_map = defaultdict(Transaction)
        self.remaining_instructions = []
        self.site_manager = SiteManager(10, 20)
        self.wait_for_graph = defaultdict(set)

    def begin(self, t_id, time):
        '''Begins a new transaction with transaction id, t_id at time, time.
        Parameters:
            self(TransactionManager): instance of the class
            t_id(string): transaction id
            time(int): time when the transaction begins
        SideEffect:
            creates a new transaction and adds it to the transaction_map
        '''           
        transaction = Transaction(t_id, TransactionStatus.READY, time)
        self.transaction_map[t_id] = transaction
        print(f'{t_id} begins')

    def beginRO(self, t_id, time):
        '''Begins a new read-only transaction with transaction id, t_id at time, time.
        Parameters:
            self(TransactionManager): instance of the class
            t_id(string): transaction id
            time(int): time when the transaction begins
        SideEffect:
            creates a new read-only transaction and adds it to the transaction_map
        '''          
        transaction = Transaction(t_id, TransactionStatus.READY, time, True)
        self.transaction_map[t_id] = transaction
        print(f'Read Only {t_id} begins')

    def read(self, t_id, var, time):
        '''Reads the value of the var for transaction, t_id at time, time.
        Parameters:
            self(TransactionManager): instance of the class
            t_id(string): transaction id
            var(string): name of the variable
            time(int): time of instruction
        SideEffect:
            Reads and prints the value of the given var.
            If variable cannot be read due to lock conflicts then blocks it and add to waiting instructions.
            Updates wait-for graph
        '''           
        if t_id not in self.transaction_map:
            print(f'{t_id} transaction is not yet started')
            return
        instruction = Instruction(t_id, InstructionType.READ, var, None, time)
        transaction = self.transaction_map[t_id]
        conflicting_transaction = self.check_conflict_in_remaining_instructions(t_id,
            instruction)
        if conflicting_transaction == None:
            val, site = self.site_manager.read(transaction, var)
            if site is not None:
                if transaction.status != TransactionStatus.ABORTED:
                    transaction.status = TransactionStatus.RUNNING
                self.transaction_map[t_id].sites_accessed.add(site)
                print(
                    f'{t_id} accessed {var} from the site: {site} having value: {val}')
                return
            else:
                self.update_wait_for_graph_with_executing_transaction(
                    t_id, var)
        else:
            self.update_wait_for_graph(t_id, conflicting_transaction)
        if transaction.status != TransactionStatus.BLOCKED and transaction.status != TransactionStatus.ABORTED:
            self.remaining_instructions.append(instruction)
            transaction.status = TransactionStatus.BLOCKED
            print(f'Blocked transaction: {t_id}')
        self.detect_and_handle_deadlock(t_id)


    def write(self, t_id, var, val, time):
        '''Write the var with value, val for transaction, t_id at time, time.
        Parameters:
            self(TransactionManager): instance of the class
            t_id(string): transaction id
            var(string): name of the variable
            val(string): value to be written
            time(int): time of instruction
        SideEffect:
            Write value, val to the variable, var.
            If variable cannot be written due to lock conflicts,then  blocks it and add to waiting instructions.
            Updates the wait-for graph
        '''             
        if t_id not in self.transaction_map:
            print(f'{t_id} transaction is not yet started')
            return
        instruction = Instruction(t_id, InstructionType.WRITE, var, val, time)
        transaction = self.transaction_map[t_id]
        conflicting_transaction = self.check_conflict_in_remaining_instructions(
            t_id, instruction)
        if conflicting_transaction == None or len(self.wait_for_graph[conflicting_transaction]) == 0: #write allowed if conflict is due to read after recovery
            sites_written = self.site_manager.write(t_id, var, val)
            if len(sites_written) > 0:
                if transaction.status != TransactionStatus.ABORTED:
                    transaction.status = TransactionStatus.RUNNING
                self.transaction_map[t_id].sites_accessed.update(sites_written)
                print(
                    f'{t_id} wrote {var} to the sites: {sites_written} with value: {val}')
                return
            else:
                self.update_wait_for_graph_with_executing_transaction(
                    t_id, var)
        else:
            self.update_wait_for_graph(t_id, conflicting_transaction)
        if transaction.status != TransactionStatus.BLOCKED and transaction.status != TransactionStatus.ABORTED:
            self.remaining_instructions.append(instruction)
            transaction.status = TransactionStatus.BLOCKED
            print(f'Blocked transaction: {t_id}')  
        self.detect_and_handle_deadlock(t_id)

    def end(self, t_id, time): 
        '''Ends the transaction t_id, either commits or aborts. Thereafter process remaining instructions
        Parameters:
            self(TransactionManager): instance of the class
            t_id(string): transaction id
            time(int): time of instruction
        '''                 
        if t_id in self.transaction_map:
            if self.transaction_map[t_id].status == TransactionStatus.ABORTED:
                print(f'Transaction:{t_id} aborts due to previous access to failed site')
                self.abort(t_id, time)
            else:
                self.commit(t_id, time)
        self.process_remaining_instructions()

    def abort(self, t_id, time):
        '''Abort the transaction. Thereafter process remaining instructions
        Parameters:
            self(TransactionManager): instance of the class
            t_id(string): transaction id
            time(int): time of instruction
        SideEffect:
            Removes the transaction, t_id from the transaction_map
        '''         
        sites_accessed = self.transaction_map[t_id].sites_accessed
        for site in sites_accessed:
            self.site_manager.abort(site, t_id, time)
        self.transaction_map.pop(t_id)
        self.remove_transaction_from_wait_for_graph(t_id)
        print(f'Transaction:{t_id} aborts')
        self.process_remaining_instructions()

    def commit(self, t_id, time):
        '''Commit the transaction. Thereafter process remaining instructions
        Parameters:
            self(TransactionManager): instance of the class
            t_id(string): transaction id
            time(int): time of instruction
        SideEffect:
            Removes the transaction, t_id from the transaction_map
        '''            
        sites_accessed = self.transaction_map[t_id].sites_accessed
        for site in sites_accessed:
            self.site_manager.commit(site, t_id, time)
        self.transaction_map.pop(t_id)
        self.remove_transaction_from_wait_for_graph(t_id)
        print(f'commited {t_id}')

    def dump(self):
        '''Print the current state of sites and committed variables. 
        Parameters:
            self(TransactionManager): instance of the class
        '''          
        self.site_manager.dump()

    def fail(self, site):
        '''Fails the given site. If a transaction has accessed this site it is marked as Aborted.

        Parameters:
            self(TransactionManager): instance of the class. 
            site_name(string): name of the site.      
        '''            
        self.site_manager.fail(site)
        for k, transaction in self.transaction_map.items():
            if site in transaction.sites_accessed:
                transaction.status = TransactionStatus.ABORTED

    def recover(self, site):
        '''Recovers the given failed site

        Parameters:
            self(TransactionManager): instance of the class. 
            site_name(string): name of the site.   
        '''          
        self.site_manager.recover(site)

    def process_remaining_instructions(self):
        '''Process the remaining instructions in blocked state.
        Parameters:
            self(TransactionManager): instance of the class. 
        SideEffect:
            Process the remaining instructions that can be processed, and keep other in the list  
        '''           
        rem_inst = []
        for inst in self.remaining_instructions:
            if inst.t_id in self.transaction_map:
                if inst.type == InstructionType.READ:
                    self.read(inst.t_id, inst.var, inst.time)
                elif inst.type == InstructionType.WRITE:
                    self.write(inst.t_id, inst.var, inst.val, inst.time)
                if self.transaction_map[inst.t_id].status == TransactionStatus.BLOCKED:
                    rem_inst.append(inst)
        self.remaining_instructions = rem_inst

    def check_conflict_in_remaining_instructions(self, t_id, instruction):
        '''Check conflicts of the current t_id with the already remaining instructions
        Parameters:
            self(TransactionManager): instance of the class. 
            t_id(string): transaction id of the instruction
            instruction(Instruction): Instruction object containing all the information regarding the instruction
        Returns:
            transaction id of conflicting instruction
        '''          
        if not self.transaction_map[t_id].read_only:
            for inst in reversed(self.remaining_instructions):
                if inst.time >= instruction.time:
                    continue
                elif inst.t_id != instruction.t_id and inst.var == instruction.var:
                    if (instruction.type == InstructionType.WRITE) or (instruction.type == InstructionType.READ and inst.type == InstructionType.WRITE):
                        return inst.t_id
        return None

    def update_wait_for_graph(self, t_id, conflicting_t_id):
        '''Updates the wait_for_graph to add an edge from t_id to conflicting_t_id
        Parameters:
            self(TransactionManager): instance of the class. 
            t_id(string): transaction id of the instruction
            conflicting_t_id(string): transaction id of the conflicting instruction
        SideEffect:
            Add an edge from t_id to conflicting_t_id in the wait_for_graph
        '''            
        if not self.transaction_map[t_id].read_only:
            self.wait_for_graph[t_id].add(conflicting_t_id)

    def update_wait_for_graph_with_executing_transaction(self, t_id, var):
        '''Updates the wait_for_graph to add an edge from t_id to all other transaction that has lock on the variable, var
        Parameters:
            self(TransactionManager): instance of the class. 
            t_id(string): transaction id of the instruction
            var(string): variable name
        SideEffect:
            Add an edge from t_id to to all other transaction that has lock on the variable, var
        '''           
        if not self.transaction_map[t_id].read_only:
            t_ids = self.site_manager.get_locking_transaction(var)
            t_ids.discard(t_id)
            self.wait_for_graph[t_id].update(t_ids)

    def remove_transaction_from_wait_for_graph(self, t_id):
        '''Removes the transaction t_id from the wait_for_graph.
        Parameters:
            self(TransactionManager): instance of the class. 
            t_id(string): transaction id of the instruction
        SideEffect:
            Removes the transaction t_id from each transactions values and remove the t_id from the wait_for_graph.
        '''         
        for t_ids in self.wait_for_graph.values():
            t_ids.discard(t_id)
        if t_id in self.wait_for_graph:
            self.wait_for_graph.pop(t_id)

    def is_deadlocked(self, t_id, vis, rec_vis):
        '''Check if the current state of wait-for-graph contains a cycle
        Parameters:
            self(TransactionManager): instance of the class. 
            t_id(string): transaction id of the instruction
            vis(set): set of nodes already visited
            rec_vis(set): set of nodes visited in current recursion
        Returns:
            True if the wait_for_graph contains cycle otherwise False
        '''          
        if t_id in rec_vis:
            return True
        if t_id in vis:
            return False
        vis.add(t_id)
        rec_vis.add(t_id)
        for v in self.wait_for_graph[t_id]:
            if self.is_deadlocked(v, vis, rec_vis):
                return True
        rec_vis.discard(t_id)
        return False

    def detect_and_handle_deadlock(self, t_id):
        '''Check if the current state of wait-for-graph leads to deadlock
        Parameters:
            self(TransactionManager): instance of the class. 
            t_id(string): transaction id of the instruction
        SideEffect:
            If deadlock if found Aborts the youngest transaction in the wait_for_graph cycle.
        '''         
        rec_vis = set()
        if self.is_deadlocked(t_id, set(), rec_vis):
            y_tid, time = self.find_youngest_transaction(rec_vis)
            print(f'Aborting transaction :{y_tid} because of deadlock')
            self.abort(y_tid, time)

    def find_youngest_transaction(self, rec_vis):
        '''find the youngest transaction in the wait_for_graph cycle.
        Parameters:
            self(TransactionManager): instance of the class. 
            rec_vis(string): sub-graph in wait-for-graph that has cycle
        Returns:
            tuple: return tuple of youngest transaction and time.
        '''         
        max_time = 0
        max_trans = ''
        for t_id in rec_vis:
            trans = self.transaction_map[t_id]
            if trans.start_time >= max_time:
                max_time = trans.start_time
                max_trans = trans.id
        return (max_trans, max_time)
