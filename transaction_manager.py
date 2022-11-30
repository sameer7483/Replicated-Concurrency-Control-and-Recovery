from transaction import Transaction
from trans_status import TransactionStatus
from collections import deque, defaultdict
from instruction import Instruction
from instruction_type import InstructionType
from site_manager import SiteManager

class TransactionManager:
    def __init__(self):
        self.transaction_map = defaultdict(Transaction)
        self.remaining_instructions = deque()
        self.site_manager = SiteManager()

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
        instruction = Instruction(t_id, InstructionType.READ, var, time)
        transaction = self.transaction_map[t_id]
        val, site = self.site_manager.read(t_id, var)
        self.transaction_map[t_id].sites_accessed.add(site)
        print(f'{t_id} accessed {var} from the site: {site} having value: {val}')
        
    def write(self, t_id, var, val, time):
        if t_id not in self.transaction_map:
            print(f'{t_id} transaction is not yet started')
            return
        instruction = Instruction(t_id, InstructionType.WRITE, var, time)
        transaction = self.transaction_map[t_id]
        sites_written = self.site_manager.write(t_id, var, val)
        self.transaction_map[t_id].sites_accessed.update(sites_written)
        print(f'{t_id} wrote {var} to the sites: {sites_written} with value: {val}')

    def end(self, t_id, time):
        self.commit(t_id, time)

    def commit(self, t_id, time):
        sites_accessed = self.transaction_map[t_id].sites_accessed
        for site in sites_accessed:
            # self.site_manager.print_lock_table(site)
            self.site_manager.commit(site, t_id, time)
            # self.site_manager.print_lock_table(site)
        print(f'commited {t_id}')
    
    def dump(self):
        self.site_manager.dump()