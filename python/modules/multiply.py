class Data:
    def __init__(self, input_dict):
        self.a = input_dict['a']
        self.b = input_dict['b']
    
    def multiply(self):
        return self.a * self.b

def process(input_dict):
    try:
        data = Data(input_dict)
    except KeyError:
        return (False, "Dados de entrada devem conter 'a' e 'b'")
    
    result = {'produto': data.multiply()}
    
    return (True, result)
