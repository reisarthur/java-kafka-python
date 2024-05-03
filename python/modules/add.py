class Data:
    def __init__(self, input_dict):
        self.a = input_dict['a']
        self.b = input_dict['b']
    
    def sum(self):
        return self.a + self.b

def process(input_dict):
    try:
        data = Data(input_dict)
    except KeyError:
        return (False, "Dados de entrada devem conter 'a' e 'b'")
    
    result = {'soma': data.sum()}
    
    return (True, result)
