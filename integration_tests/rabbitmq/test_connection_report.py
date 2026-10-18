"""The RabbitMQ source's connection report: the queue, its ready messages and consumers."""
import io
from contextlib import redirect_stdout


def check(settings, queue):
    from mage_ai.data_preparation.models.block.streaming import check_connection

    out = io.StringIO()
    with redirect_stdout(out):
        check_connection('data_loader', (
            f"connector_type: rabbitmq\nconnection_host: {settings['connection_host']}\n"
            f"connection_port: {settings['connection_port']}\nqueue_name: {queue}\n"
            f"username: {settings['username']}\npassword: {settings['password']}\n"
        ))
    return out.getvalue()


def test_the_report_counts_the_queue_or_says_it_is_missing(
    rabbitmq_settings, rabbitmq_channel, rabbitmq_queue,
):
    for i in range(3):
        rabbitmq_channel.basic_publish(exchange='', routing_key=rabbitmq_queue,
                                       body=f'{{"n": {i}}}'.encode())

    report = check(rabbitmq_settings, rabbitmq_queue)
    missing = check(rabbitmq_settings, f'{rabbitmq_queue}_nowhere')

    assert f'Queue {rabbitmq_queue}: 3 messages ready, 0 consumers.' in report
    assert 'missing on the broker' in missing
    # The check read nothing: the messages are still there.
    assert rabbitmq_channel.queue_declare(rabbitmq_queue, passive=True).method \
        .message_count == 3
