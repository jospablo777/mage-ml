"""
A Kafka broker down for minutes under a running stream (make test-soak): longer than the
group's session timeout and librdkafka's message timeout (300 s), so the group takes the
partitions away and the sink's writes in flight fail after their batches were dropped. The
stream gets its partitions back and nothing is lost; the writer once waited for the retry
of a dropped batch, which never comes, and the stream stood still (I-202).
"""
from integration_tests.kafka.test_broker_restart import (  # noqa: F401 (fixture)
    TOTAL,
    broker,
    restart_under_stream,
)


def test_a_broker_down_past_the_message_timeout_loses_nothing(
    broker, mage_project, tmp_path,  # noqa: F811
):
    restart_under_stream(broker, mage_project, tmp_path, ['kill'], 330, TOTAL // 4,
                         recovery=300)
