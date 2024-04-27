from kafka import KafkaConsumer, KafkaProducer

def import_module(module_name):
    import importlib.util
    try:
        module_path = f'modules/{module_name}.py'
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except FileNotFoundError:
        return


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

        print(f'Mensagem recebida: key={msg.key}, value={msg.value}, headers={msg.headers}')
        value = eval(msg.value)

        module = import_module(msg.key)
        if module:
            value['result'] = module.process(value['data'])
        else:
            value['result'] = 'Módulo não encontrado.'

        producer.send(output_topic, key=msg.key, value=str(value), headers=msg.headers)
        producer.flush()

        print(f'Mensagem enviada: key={msg.key}, value={str(value)}, headers={msg.headers}')
