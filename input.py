import fileinput
import sys
from status import Status
from variable import Variable
from lock import Lock
from lock_type import LockType
from site_manager import Site, SiteManager
from collections import defaultdict
from transaction_manager import TransactionManager
def parse_input(line):
    command = line[:line.index('(')]
    arg_string = line[line.index('(')+1:line.index(')')]
    args = [arg.strip() for arg in arg_string.split(',')]
    return (command, args)

def main():
    n = len(sys.argv)
    # num_site = 10
    # num_var = 20
    # sites = defaultdict(Site)
    # for i in range(1, num_site+1):
    #     variables = defaultdict(Variable)
    #     for j in range(1, num_var+1):
    #         if j % 2 == 0 or (j % 2 != 0 and (1+ j%10) == i):
    #             var = Variable('x'+str(j), 10*j, Lock(LockType.NO_LOCK))
    #             variables['x'+str(j)] = var             
    #     s = Site(str(i),Status.READY, variables)
    #     sites[str(i)] = s
    # site_manager = SiteManager(sites)
    transaction_manager = TransactionManager()
    time = 0
    if n > 1:
        file_name = sys.argv[1]
        for line in fileinput.input(files = file_name):
            time += 1
            print(parse_input(line))
            command, args = parse_input(line)
            if command == 'begin':
                t_id = args[0]
                print(t_id)
                transaction_manager.begin(t_id, time)
            elif command == 'beginRO':
                t_id = args[0]
            elif command == 'W':
                t_id = args[0]
                var = args[1]
                val = args[2]
                transaction_manager.write(t_id, var, val, time)
            elif command == 'R':
                t_id = args[0]
                var = args[1]
                transaction_manager.read(t_id, var, time)
            elif command == 'end':
                t_id = args[0]
                transaction_manager.end(t_id, time)
            elif command == 'fail':
                s_id = args[0]
            elif command == 'recover':
                s_id = args[0]
            elif command == 'dump':
                transaction_manager.dump()
            else:
                print("incorrect input")
                exit()
          
main()
    
