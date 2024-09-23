import pika
import os
import json
import time

def main():
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))

   
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port)
    )
    channel = connection.channel()

    exchange_name = 'leads_exchange'

    
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)

    
    queue_name = 'scrapper_queue'
    channel.queue_declare(queue=queue_name, durable=True)

   
    channel.queue_bind(exchange=exchange_name, queue=queue_name)

    print(" [*] Esperando por mensagens. Para sair pressione CTRL+C")

    def callback(ch, method, properties, body):
        try:
            lead_data = json.loads(body)
            print(f" [x] Recebido Lead para Scrapper: {json.dumps(lead_data, indent=4, ensure_ascii=False)}")
            
        except json.JSONDecodeError as e:
            print(f" [!] Erro ao decodificar JSON: {e}")

    # Consumir mensagens da fila
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(" [x] Interrompido pelo usuário")
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == "__main__":
    main()
