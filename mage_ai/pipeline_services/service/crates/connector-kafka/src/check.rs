//! The connection check of a Kafka source or sink: the brokers, each topic's partitions,
//! messages and retention, and for a source the consumer group's committed lag. It reads
//! committed offsets without joining the group, so a running stream is not rebalanced.

use std::time::Duration;

use mage_stream_ports::check::{ConnectionReport, human_duration};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::Value;

use crate::{client_config, text};

const WITHIN: Duration = Duration::from_secs(10);

/// Checks a source (`source`) or a sink's settings.
pub async fn check(config: &Value, source: bool) -> ConnectionReport {
    let config = config.clone();
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || check_blocking(&config, source, &runtime))
        .await
        .unwrap_or_else(|e| ConnectionReport::failed(format!("The check stopped: {e}")))
}

fn topics(config: &Value, source: bool) -> Vec<String> {
    if source && let Some(Value::Array(list)) = config.get("topics") {
        let topics: Vec<String> = list
            .iter()
            .filter_map(|t| t.as_str().map(String::from))
            .collect();
        if !topics.is_empty() {
            return topics;
        }
    }
    text(config, "topic").into_iter().collect()
}

fn refused(error: &KafkaError) -> Option<&'static str> {
    match error.rdkafka_error_code()? {
        RDKafkaErrorCode::SaslAuthenticationFailed | RDKafkaErrorCode::Authentication => {
            Some("the brokers refused the credentials")
        }
        RDKafkaErrorCode::TopicAuthorizationFailed => Some("not authorized for the topic"),
        RDKafkaErrorCode::GroupAuthorizationFailed => Some("not authorized for the consumer group"),
        _ => None,
    }
}

fn check_blocking(
    config: &Value,
    source: bool,
    runtime: &tokio::runtime::Handle,
) -> ConnectionReport {
    let topics = topics(config, source);
    if topics.is_empty() {
        return ConnectionReport::failed(if source {
            "The settings name no topic (topic or topics)."
        } else {
            "The settings name no topic."
        });
    }
    let mut client = match client_config(config) {
        Ok(client) => client,
        Err(error) => return ConnectionReport::failed(error.to_string()),
    };
    let servers = client
        .get("bootstrap.servers")
        .unwrap_or("the brokers")
        .to_string();
    client
        .set("socket.timeout.ms", "10000")
        .set("socket.connection.setup.timeout.ms", "10000")
        .set("enable.auto.commit", "false");
    let group = if source {
        text(config, "consumer_group")
    } else {
        None
    };
    if let Some(group) = &group {
        client.set("group.id", group);
    }
    let consumer: BaseConsumer = match client.create() {
        Ok(consumer) => consumer,
        Err(error) => return ConnectionReport::failed(format!("Kafka: {error}")),
    };
    let metadata = match consumer.fetch_metadata(Some(&topics[0]), WITHIN) {
        Ok(metadata) => metadata,
        Err(error) => {
            return ConnectionReport::failed(match refused(&error) {
                Some(reason) => format!("Reached {servers}, but {reason}: {error}"),
                None => format!("No broker answered at {servers} within 10 seconds: {error}"),
            });
        }
    };
    let mut report = ConnectionReport::passed(format!(
        "Reached {} broker{} at {servers}.",
        metadata.brokers().len(),
        if metadata.brokers().len() == 1 {
            ""
        } else {
            "s"
        }
    ));
    let admin: Option<AdminClient<DefaultClientContext>> = client.create().ok();
    for topic in &topics {
        report = check_topic(
            report,
            &consumer,
            admin.as_ref(),
            runtime,
            config,
            topic,
            group.as_deref(),
            source,
        );
    }
    report
}

#[allow(clippy::too_many_arguments)]
fn check_topic(
    mut report: ConnectionReport,
    consumer: &BaseConsumer,
    admin: Option<&AdminClient<DefaultClientContext>>,
    runtime: &tokio::runtime::Handle,
    config: &Value,
    topic: &str,
    group: Option<&str>,
    source: bool,
) -> ConnectionReport {
    let metadata = match consumer.fetch_metadata(Some(topic), WITHIN) {
        Ok(metadata) => metadata,
        Err(error) => return report.fail(format!("Topic {topic}: {error}")),
    };
    let Some(found) = metadata.topics().iter().find(|t| t.name() == topic) else {
        return report.fail(format!("Topic {topic}: the brokers did not describe it."));
    };
    if let Some(error) = found.error() {
        let code = RDKafkaErrorCode::from(error);
        return match code {
            RDKafkaErrorCode::UnknownTopicOrPartition if !source => report.warn(format!(
                "Topic {topic} does not exist; the first write creates it only where the \
                 brokers allow that (auto.create.topics.enable)."
            )),
            RDKafkaErrorCode::UnknownTopicOrPartition => {
                report.fail(format!("Topic {topic} does not exist."))
            }
            RDKafkaErrorCode::TopicAuthorizationFailed => {
                report.fail(format!("Not authorized to describe topic {topic}."))
            }
            other => report.fail(format!("Topic {topic}: {other}")),
        };
    }
    let partitions: Vec<i32> = found.partitions().iter().map(|p| p.id()).collect();
    report = report.fact(&format!("{topic}: partitions"), partitions.len());
    let mut ends = Vec::with_capacity(partitions.len());
    let mut kept = 0i64;
    for &partition in &partitions {
        match consumer.fetch_watermarks(topic, partition, WITHIN) {
            Ok((low, high)) => {
                kept += high - low;
                ends.push((partition, high));
            }
            Err(error) => {
                return report.fail(format!("Topic {topic} partition {partition}: {error}"));
            }
        }
    }
    report = report.fact(&format!("{topic}: messages kept"), kept);
    if let Some(admin) = admin {
        let options = AdminOptions::new().request_timeout(Some(WITHIN));
        let described =
            runtime.block_on(admin.describe_configs(&[ResourceSpecifier::Topic(topic)], &options));
        let retention = described
            .ok()
            .and_then(|mut results| results.pop())
            .and_then(Result::ok)
            .and_then(|resource| resource.get("retention.ms").and_then(|e| e.value.clone()))
            .and_then(|value| value.parse::<i64>().ok());
        report = report.fact(
            &format!("{topic}: retention"),
            retention.map_or(
                "unknown (not allowed to describe the topic)".into(),
                human_duration,
            ),
        );
    }
    let Some(group) = group else {
        return report;
    };
    let mut list = TopicPartitionList::new();
    for &partition in &partitions {
        list.add_partition(topic, partition);
    }
    let committed = match consumer.committed_offsets(list, WITHIN) {
        Ok(committed) => committed,
        Err(error) => {
            return match refused(&error) {
                Some(reason) => report.fail(format!("Consumer group {group}: {reason}.")),
                None => report.fail(format!("Consumer group {group}: {error}")),
            };
        }
    };
    let mut lag = 0i64;
    let mut with_position = 0;
    for (partition, end) in &ends {
        if let Some(element) = committed.find_partition(topic, *partition)
            && let Offset::Offset(position) = element.offset()
        {
            lag += (end - position).max(0);
            with_position += 1;
        }
    }
    report = report.fact(&format!("{topic}: consumer group"), group);
    if with_position == 0 {
        let start = text(config, "auto_offset_reset").unwrap_or_else(|| "latest".into());
        report.warn(format!(
            "Consumer group {group} has no committed position on {topic}: the stream starts \
             at the {start} message."
        ))
    } else {
        report = report.fact(&format!("{topic}: lag"), lag);
        if with_position < ends.len() {
            report = report.warn(format!(
                "Consumer group {group} has a position on {with_position} of {} partitions of \
                 {topic}.",
                ends.len()
            ));
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use mage_stream_ports::check::CheckResult;
    use serde_json::json;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_broker_that_does_not_answer_fails_the_check_in_its_words() {
        let report = check(
            &json!({"bootstrap_server": "127.0.0.1:1", "topic": "t",
                                   "consumer_group": "g"}),
            true,
        )
        .await;
        assert_eq!(report.result, CheckResult::Failed);
        assert!(
            report.summary.contains("No broker answered at 127.0.0.1:1"),
            "{}",
            report.summary
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn settings_without_a_topic_fail_before_connecting() {
        let report = check(&json!({"bootstrap_server": "127.0.0.1:1"}), false).await;
        assert_eq!(report.result, CheckResult::Failed);
        assert!(report.summary.contains("no topic"));
    }

    /// Against a broker (`MAGE_TEST_KAFKA_BOOTSTRAP`, integration_tests/compose.yaml serves
    /// localhost:19092): a topic of 5 messages, a group that committed 2.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_source_reports_its_topic_and_the_groups_lag() {
        use rdkafka::admin::NewTopic;
        use rdkafka::consumer::CommitMode;
        use rdkafka::producer::{FutureProducer, FutureRecord};

        let Ok(servers) = std::env::var("MAGE_TEST_KAFKA_BOOTSTRAP") else {
            eprintln!("MAGE_TEST_KAFKA_BOOTSTRAP is not set: skipped");
            return;
        };
        let topic = format!("check_{}", std::process::id());
        let mut client = rdkafka::ClientConfig::new();
        client.set("bootstrap.servers", &servers);
        let admin: AdminClient<DefaultClientContext> = client.create().unwrap();
        admin
            .create_topics(
                &[
                    NewTopic::new(&topic, 2, rdkafka::admin::TopicReplication::Fixed(1))
                        .set("retention.ms", "604800000"),
                ],
                &AdminOptions::new(),
            )
            .await
            .unwrap();
        let producer: FutureProducer = client.create().unwrap();
        for i in 0..5 {
            producer
                .send(
                    FutureRecord::<str, str>::to(&topic)
                        .partition(0)
                        .payload(&i.to_string()),
                    WITHIN,
                )
                .await
                .unwrap();
        }
        let group = format!("{topic}_group");
        let settings =
            json!({"bootstrap_server": servers, "topic": topic, "consumer_group": group});
        let report = check(&settings, true).await;
        assert_eq!(report.result, CheckResult::Warning, "{report:?}");
        assert!(
            report.problems[0].contains("no committed position"),
            "{report:?}"
        );

        let mut committer = client.clone();
        committer.set("group.id", &group);
        let consumer: BaseConsumer = committer.create().unwrap();
        let mut position = TopicPartitionList::new();
        position
            .add_partition_offset(&topic, 0, Offset::Offset(2))
            .unwrap();
        position
            .add_partition_offset(&topic, 1, Offset::Offset(0))
            .unwrap();
        consumer.commit(&position, CommitMode::Sync).unwrap();

        let report = check(&settings, true).await;
        assert_eq!(report.result, CheckResult::Passed, "{report:?}");
        let fact = |label: &str| {
            report
                .facts
                .iter()
                .find(|(l, _)| l == &format!("{topic}: {label}"))
                .map(|(_, v)| v.clone())
        };
        assert_eq!(fact("partitions").as_deref(), Some("2"));
        assert_eq!(fact("messages kept").as_deref(), Some("5"));
        assert_eq!(fact("retention").as_deref(), Some("7 days"));
        assert_eq!(fact("lag").as_deref(), Some("3"));

        let missing = check(
            &json!({"bootstrap_server": servers, "topic": "no_such_topic_x"}),
            false,
        )
        .await;
        assert_eq!(missing.result, CheckResult::Warning, "{missing:?}");
        let missing = check(
            &json!({"bootstrap_server": servers, "topic": "no_such_topic_x",
                                    "consumer_group": "g"}),
            true,
        )
        .await;
        assert_eq!(missing.result, CheckResult::Failed, "{missing:?}");
    }
}
