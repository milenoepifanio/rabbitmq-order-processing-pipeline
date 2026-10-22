
import json
import pika

# Conecta ao RabbitMQ local
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

channel = connection.channel()

# Declara a fila
channel.queue_declare(
    queue="orders",
    durable=True
)
# Cria um pedido fictício
order = {
    "order_id": 1001,
    "customer_id": 501,
    "product": "Notebook",
    "quantity": 1,
    "status": "created"
}

# Converte o pedido em JSON
message = json.dumps(order)

# Publica a mensagem
channel.basic_publish(
    exchange="",
    routing_key="orders",
    body=message
)

print(f"Pedido enviado: {message}")

connection.close()
