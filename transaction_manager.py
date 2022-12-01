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
        print(f'Read Only {t_id} begins at time {time}')

    def read(self, t_id, var, time):
        if t_id not in self.transaction_map:
            print(f'{t_id} transaction is not yet started')
            return
        instruction = Instruction(t_id, InstructionType.READ, var, None, time)
        transaction = self.transaction_map[t_id]
        conflicting_transaction = self.check_conflict_in_remaining_instructions(
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
        if transaction.status != TransactionStatus.BLOCKED:
            self.remaining_instructions.append(instruction)
            transaction.status = TransactionStatus.BLOCKED
            print(f'Blocked transaction: {t_id}')
        self.detect_and_handle_deadlock(t_id)


    def write(self, t_id, var, val, time):
        if t_id not in self.transaction_map:
            print(f'{t_id} transaction is not yet started')
            return
        instruction = Instruction(t_id, InstructionType.WRITE, var, val, time)
        transaction = self.transaction_map[t_id]
        conflicting_transaction = self.check_conflict_in_remaining_instructions(
            instruction)
        # print(conflicting_transaction)
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
        if transaction.status != TransactionStatus.BLOCKED:
            self.remaining_instructions.append(instruction)
            transaction.status = TransactionStatus.BLOCKED
            print(f'Blocked transaction: {t_id}')
        self.detect_and_handle_deadlock(t_id)

    def end(self, t_id, time):
        if t_id in self.transaction_map:
            if self.transaction_map[t_id].status == TransactionStatus.ABORTED:
                print(f'Transaction:{t_id} aborts due to previous access to failed site')
                self.abort(t_id, time)
            else:
                self.commit(t_id, time)
        self.process_remaining_instructions()

    def abort(self, t_id, time):
        sites_accessed = self.transaction_map[t_id].sites_accessed
        for site in sites_accessed:
            self.site_manager.abort(site, t_id, time)
        self.transaction_map.pop(t_id)
        self.remove_transaction_from_wait_for_graph(t_id)
        print(f'Transaction:{t_id} aborts')
        self.process_remaining_instructions()

    def commit(self, t_id, time):
        sites_accessed = self.transaction_map[t_id].sites_accessed
        for site in sites_accessed:
            self.site_manager.commit(site, t_id, time)
        self.transaction_map.pop(t_id)
        self.remove_transaction_from_wait_for_graph(t_id)
        print(f'commited {t_id}')

    def dump(self):
        self.site_manager.dump()

    def fail(self, site):
        self.site_manager.fail(site)
        # self.site_manager.print_all_site_status()
        for k, transaction in self.transaction_map.items():
            print(transaction.name)
            if site in transaction.sites_accessed:
                transaction.status = TransactionStatus.ABORTED

    def recover(self, site):
        self.site_manager.recover(site)
        # self.site_manager.print_all_site_status()

    def process_remaining_instructions(self):
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

    def check_conflict_in_remaining_instructions(self, instruction):
        for inst in reversed(self.remaining_instructions):
            if inst.time >= instruction.time:
                continue
            elif inst.t_id != instruction.t_id and inst.var == instruction.var:
                if (instruction.type == InstructionType.WRITE) or (instruction.type == InstructionType.READ and inst.type == InstructionType.WRITE):
                    return inst.t_id
        return None

    def update_wait_for_graph(self, t_id, conflicting_t_id):
        self.wait_for_graph[t_id].add(conflicting_t_id)

    def update_wait_for_graph_with_executing_transaction(self, t_id, var):
        t_ids = self.site_manager.get_locking_transaction(var)
        t_ids.discard(t_id)
        self.wait_for_graph[t_id].update(t_ids)

    def remove_transaction_from_wait_for_graph(self, t_id):
        for t_ids in self.wait_for_graph.values():
            t_ids.discard(t_id)
        if t_id in self.wait_for_graph:
            self.wait_for_graph.pop(t_id)

    def is_deadlocked(self, t_id, vis, rec_vis):
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
        rec_vis = set()
        if self.is_deadlocked(t_id, set(), rec_vis):
            y_tid, time = self.find_youngest_transaction(rec_vis)
            print(f'Aborting transaction :{y_tid} because of deadlock')
            self.abort(y_tid, time)

    def find_youngest_transaction(self, rec_vis):
        max_time = 0
        max_trans = ''
        for t_id in rec_vis:
            trans = self.transaction_map[t_id]
            if trans.start_time >= max_time:
                max_time = trans.start_time
                max_trans = trans.name
        return (max_trans, max_time)