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
    '''Parses the input line

    Parameters:
        line(string): single line from the input file
    Returns:
        tuple: a tuple of the command and list of args
        args length varies based on the Read, Write, dump commands
    '''
    command = line[:line.index('(')]
    arg_string = line[line.index('(')+1:line.index(')')]
    args = [arg.strip() for arg in arg_string.split(',')]
    return (command, args)


def main():
    '''Starting point of the application'''    
    n = len(sys.argv)
    transaction_manager = TransactionManager()
    time = 0
    if n > 1:
        file_name = sys.argv[1]
        for line in fileinput.input(files=file_name):
            if line.startswith('//'):
                continue
            time += 1
            line = line.strip('\n')
            print(line)
            command, args = parse_input(line)
            if command == 'begin':
                t_id = args[0]
                transaction_manager.begin(t_id, time)
            elif command == 'beginRO':
                t_id = args[0]
                transaction_manager.beginRO(t_id, time)
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
                transaction_manager.fail(s_id)
            elif command == 'recover':
                s_id = args[0]
                transaction_manager.recover(s_id)
            elif command == 'dump':
                transaction_manager.dump()
            else:
                print("incorrect input")
                exit()
            print('\n')

main()
