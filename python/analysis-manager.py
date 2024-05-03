import importlib.util
from kafka import KafkaConsumer, KafkaProducer

def import_module(module_name):
    module_path = f'modules/{module_name}.py'
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


servers = ['localhost:9092']
input_topic = 'analysis.request'
output_topic = 'analysis.response'

consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers = servers,
    key_deserializer = lambda x: x.decode('utf-8'),
    value_deserializer = lambda x: x.decode('utf-8'),
    auto_offset_reset = 'latest'
)

producer = KafkaProducer(
    bootstrap_servers = servers,
    key_serializer = lambda x: x.encode('utf-8'),
    value_serializer = lambda x: x.encode('utf-8')
)

print('Producer and Consumer created')


while True:

    for msg in consumer:

        try:
            print(f'Mensagem recebida: key={msg.key}, headers={msg.headers}, value={msg.value}')
            module = import_module(msg.key)
            value = eval(msg.value)
            value['success'], value['result'] = module.process(value['data'])

        except FileNotFoundError:
            value['success'], value['result'] = (False, f'Módulo não encontrado: "{msg.key}"')

        except SyntaxError:
            value = {}
            value['success'], value['result'] = (False, f'Mensagem json mal formada: "{msg.value}"')

        except KeyError:
            value['success'], value['result'] = (False, f'Mensagem deve conter o atributo "data": "{msg.value}"')

        except:
            value['success'], value['result'] = (False, f'Erro não reconhecido')

        finally:
            producer.send(output_topic, key=msg.key, value=str(value), headers=msg.headers)
            producer.flush()
            print(f'Mensagem enviada:  key={msg.key}, headers={msg.headers}, value={str(value)}')
