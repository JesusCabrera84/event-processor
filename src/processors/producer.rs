use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::to_vec;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::config::KafkaConfig;
use crate::processors::event_processor::Event;

/// A thin producer service that sends events to the `unit-events` topic.
#[derive(Clone)]
pub struct ProducerService {
    inner: Arc<FutureProducer>,
    topic: String,
}

impl ProducerService {
    pub fn new(cfg: &KafkaConfig) -> Result<Self> {
        let mut client = ClientConfig::new();

        // producer may use dedicated brokers/credentials; fall back to global config
        let brokers = cfg.producer_brokers.as_ref().unwrap_or(&cfg.brokers);

        client.set("bootstrap.servers", brokers);
        // Distinguish producer client in broker logs and librdkafka messages
        client.set("client.id", "event-processor-producer");

        client
            .set("enable.idempotence", "true")
            .set("acks", "all")
            // very high retries to approximate "infinite" retry behaviour
            .set("retries", "1000000")
            .set("message.timeout.ms", "300000");

        // producer-specific creds take precedence if present
        if let Some(sec) = cfg
            .producer_security_protocol
            .as_ref()
            .or(cfg.security_protocol.as_ref())
        {
            client.set("security.protocol", sec);
        }

        if let Some(mech) = cfg
            .producer_sasl_mechanism
            .as_ref()
            .or(cfg.sasl_mechanism.as_ref())
        {
            client.set("sasl.mechanisms", mech);
        }

        if let Some(user) = cfg.producer_username.as_ref().or(cfg.username.as_ref()) {
            client.set("sasl.username", user);
        }

        if let Some(pass) = cfg.producer_password.as_ref().or(cfg.password.as_ref()) {
            client.set("sasl.password", pass);
        }

        // enable useful logging integration
        client.set_log_level(rdkafka::config::RDKafkaLogLevel::Info);

        let producer = client
            .create()
            .map_err(|e| anyhow!("producer create: {e}"))?;

        // topic: explicit producer_topic -> fallback to requested hard-coded "unit-events"
        let topic = cfg
            .producer_topic
            .clone()
            .unwrap_or_else(|| "unit-events".to_string());

        tracing::info!(brokers = %brokers, topic = %topic, "kafka producer initialized");

        Ok(Self {
            inner: Arc::new(producer),
            topic,
        })
    }

    /// Produce an event asynchronously. Retries on failure with exponential backoff.
    pub async fn produce_event(&self, event: Event) {
        // TODO: persist to outbox before sending to Kafka
        // let outbox_item = OutboxEvent { ... };
        // outbox_repo.save(&outbox_item).await?;

        let key = event.event_id.to_string();

        match to_vec(&event) {
            Ok(payload_bytes) => {
                let mut attempt: u32 = 0;
                let mut backoff = Duration::from_millis(500);

                loop {
                    attempt = attempt.saturating_add(1);

                    let record = FutureRecord::to(&self.topic)
                        .key(key.as_str())
                        .payload(&payload_bytes);

                    let send_future = self.inner.send(record, Duration::from_secs(30));

                    match send_future.await {
                        Ok((_partition, _offset)) => {
                            info!(event_id = %key, attempts = attempt, "event produced");
                            break;
                        }
                        Err((kafka_error, _owned_message)) => {
                            warn!(error = ?kafka_error, attempt = attempt, "delivery error, will retry");
                        }
                    }

                    // exponential backoff with a cap
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
            Err(err) => {
                error!(error = %err, "failed to serialize event for producing");
            }
        }
    }
}
