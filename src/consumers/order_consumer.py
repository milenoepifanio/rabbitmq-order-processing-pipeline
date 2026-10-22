
import json
import pika

# Conecta ao RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

channel = connection.channel()

# Declara a mesma fila do Producer
channel.queue_declare(
    queue="orders",
    durable=True
)
# Função executada ao receber uma mensagem
def process_order(ch, method, properties, body):
    order = json.loads(body)
    print(f"Pedido recebido: {order}")

# Configura o consumo da fila
channel.basic_consume(
    queue="orders",
    on_message_callback=process_order,
    auto_ack=True
)

print("Aguardando pedidos...")

channel.start_consuming()
