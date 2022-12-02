class Instruction:
    '''Data Model for the Instruction'''
    def __init__(self, t_id, ins_type, var, val, time):
        '''creates and initialises a new Instruction

        Parameters:
        t_id(string): name of the transaction
        ins_type(InstructionType): type of Instruction i.e READ, WRITE
        var(string): variable on which this instruction applies
        val(int): value of the var that this instruction needs to update
        time(int): time when the instruction is created

        Returns:
        Instruction: a new instruction object initialized with the given values
        '''              
        self.t_id = t_id
        self.type = ins_type
        self.var = var
        self.val = val
        self.time = time
