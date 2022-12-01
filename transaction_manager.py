from transaction import Transaction
from trans_status import TransactionStatus
from collections import deque, defaultdict
from instruction import Instruction
from instruction_type import InstructionType
from site_manager import SiteManager

class TransactionManager:
    def __init__(self):
        self.transaction_map = defaultdict(Transaction)
        self.remaining_instructions = []
        self.site_manager = SiteManager()
        self.wait_for_graph = defaultdict(set)

    def begin(self, t_id, time):
        transaction = Transaction(t_id, TransactionStatus.READY, time)
        self.transaction_map[t_id] = transaction
        print(f'{t_id} begins at time {time}')

    def beginRO(self, t_id, time):
        transaction = Transaction(t_id, TransactionStatus.READY, time, True)
        self.transaction_map[t_id] = transaction

    def read(self, t_id, var, time):
        if t_id not in self.transaction_map:
            print(f'{t_id} transaction is not yet started')
            return
        instruction = Instruction(t_id, InstructionType.READ, var, None, time)
        transaction = self.transaction_map[t_id]
        conflicting_transaction = check_conflict_in_remaining_instructions(instruction)
        if conflicting_transaction == None:
            val, site = self.site_manager.read(t_id, var)
            if site is not None:
                transaction.status = TransactionStatus.RUNNING
                self.transaction_map[t_id].sites_accessed.add(site)
                print(f'{t_id} accessed {var} from the site: {site} having value: {val}')
            else:
                self.remaining_instructions.append(instruction)
                transaction.status = TransactionStatus.BLOCKED
                print(f'Blocked transaction: {t_id}')
        else:
            update_wait_for_graph(t_id, conflicting_transaction)

        
    def write(self, t_id, var, val, time):
        if t_id not in self.transaction_map:
            print(f'{t_id} transaction is not yet started')
            return
        instruction = Instruction(t_id, InstructionType.WRITE, var, val, time)
        transaction = self.transaction_map[t_id]
        sites_written = self.site_manager.write(t_id, var, val)
        if len(sites_written) > 0:
            transaction.status = TransactionStatus.RUNNING
            self.transaction_map[t_id].sites_accessed.update(sites_written)
            print(f'{t_id} wrote {var} to the sites: {sites_written} with value: {val}')
        else:
            self.remaining_instructions.append(instruction)
            transaction.status = TransactionStatus.BLOCKED
            print(f'Blocked transaction: {t_id}')            


    def end(self, t_id, time):
        if self.transaction_map[t_id].status == TransactionStatus.ABORTED:
            print(f'Transaction:{t_id} aborts')
        else:    
            self.commit(t_id, time)
        self.process_remaining_instructions()

    def commit(self, t_id, time):
        if self.transaction_map[t_id].status != TransactionStatus.ABORTED:
            sites_accessed = self.transaction_map[t_id].sites_accessed
            for site in sites_accessed:
                self.site_manager.commit(site, t_id, time)
            self.transaction_map.pop(t_id)
            print(f'commited {t_id}')
    
    def dump(self):
        self.site_manager.dump()

    def fail(self, site):
        self.site_manager.fail(site)
        for k, transaction in self.transaction_map.items():
            if site in transaction.sites_accessed:
                transaction.status = TransactionStatus.ABORTED

    def recover(self, site):
        self.site_manager.recover(site)

    def process_remaining_instructions(self):
        rem_inst = []
        for inst in self.remaining_instructions:
            if inst.type == InstructionType.READ:
                self.read(inst.t_id, inst.var, inst.time)
            elif inst.type == InstructionType.WRITE:
                self.write(inst.t_id, inst.var,inst.val, inst.time)
            if self.transaction_map[inst.t_id].status == TransactionStatus.BLOCKED:
                rem_inst.append(inst)
        self.remaining_instructions = rem_inst

    def check_conflict_in_remaining_instructions(self, instruction):
        for inst in reversed(self.remaining_instructions):
            if inst.time >= instruction.time:
                continue
            elif inst.t_id != instruction.t_id and inst.var_name == instruction.var_name:
                if (instruction.type == InstructionType.WRITE) or (instruction.type == InstructionType.READ and inst.type == InstructionType.WRITE):
                    return inst.t_id
        return None

    def update_wait_for_graph(t_id, conflicting_t_id):
        self.wait_for_graph[t_id].add(conflicting_t_id)