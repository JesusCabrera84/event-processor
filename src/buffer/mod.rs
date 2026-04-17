use std::sync::Arc;
use std::time::Duration;

use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

use crate::circuit_breaker::CircuitBreaker;
use crate::db::Database;
use crate::health::HealthTracker;
use crate::models::{CompletionStatus, Event as ModelEvent, PersistRequest};
use crate::processors::event_processor::{
    Event as KafkaEvent, Source as KafkaSource, Unit as KafkaUnit,
};
use crate::processors::producer::ProducerService;
use chrono::Utc;
use uuid::Uuid;

#[allow(clippy::too_many_arguments)]
pub fn spawn_buffer_writer(
    batch_size: usize,
    batch_timeout: Duration,
    db: Arc<Database>,
    db_breaker: Arc<CircuitBreaker>,
    health: Arc<HealthTracker>,
    mut persist_rx: mpsc::Receiver<PersistRequest>,
    completion_tx: mpsc::Sender<CompletionStatus>,
    producer: Option<Arc<ProducerService>>,
    event_type_lookup: Option<Arc<HashMap<Uuid, String>>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = time::interval(batch_timeout);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut pending = Vec::new();
        let mut buffered_events = 0usize;

        loop {
            tokio::select! {
                maybe_request = persist_rx.recv() => {
                    match maybe_request {
                        Some(request) => {
                            buffered_events += request.events.len();
                            pending.push(request);

                                if buffered_events >= batch_size {
                                flush_pending(&db, &db_breaker, &health, &completion_tx, &mut pending, &mut buffered_events, producer.clone(), event_type_lookup.clone()).await;
                            }
                        }
                        None => {
                            if !pending.is_empty() {
                                flush_pending(&db, &db_breaker, &health, &completion_tx, &mut pending, &mut buffered_events, producer.clone(), event_type_lookup.clone()).await;
                            }
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if !pending.is_empty() {
                        flush_pending(&db, &db_breaker, &health, &completion_tx, &mut pending, &mut buffered_events, producer.clone(), event_type_lookup.clone()).await;
                    }
                }
            }
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn flush_pending(
    _db: &Database,
    _db_breaker: &CircuitBreaker,
    _health: &HealthTracker,
    completion_tx: &mpsc::Sender<CompletionStatus>,
    pending: &mut Vec<PersistRequest>,
    buffered_events: &mut usize,
    producer_opt: Option<Arc<ProducerService>>,
    event_type_lookup: Option<Arc<HashMap<Uuid, String>>>,
) {
    if pending.is_empty() {
        return;
    }

    // Drain pending requests and produce directly to Kafka. We no longer persist
    // events to the DB; instead we mark completions and enqueue Kafka production
    // (best-effort).
    let drained: Vec<PersistRequest> = std::mem::take(pending);
    let event_count = drained
        .iter()
        .map(|request| request.events.len())
        .sum::<usize>();
    let flattened = drained
        .iter()
        .flat_map(|request| request.events.iter().cloned())
        .collect::<Vec<ModelEvent>>();

    // Signal success for each original PersistRequest so the caller can commit
    // offsets or consider the message handled.
    for request in drained {
        let _ = completion_tx
            .send(CompletionStatus {
                token: request.token,
                success: true,
            })
            .await;
    }

    // Produce to Kafka (best-effort). Keep behaviour mostly unchanged from
    // previous flow: spawn tasks per event and log counts.
    if let Some(producer) = producer_opt {
        for ev in flattened.into_iter() {
            let producer = producer.clone();
            let lookup = event_type_lookup.clone();
            tokio::spawn(async move {
                let event_type = lookup
                    .as_ref()
                    .and_then(|map| map.get(&ev.event_type_id).cloned())
                    .unwrap_or_else(|| ev.event_type_id.to_string());

                let mut payload = ev.payload.clone();
                if let serde_json::Value::Object(ref mut map) = payload {
                    map.remove("alert");
                    map.remove("msg_class");
                }

                let kafka_event = KafkaEvent {
                    event_id: ev.id,
                    event_type_id: ev.event_type_id,
                    schema_version: 1,
                    event_type,
                    source: KafkaSource {
                        r#type: ev.source_type.clone(),
                        id: ev.source_id.clone(),
                        message_id: ev.source_message_id.unwrap_or_else(Uuid::new_v4),
                    },
                    unit: KafkaUnit {
                        id: ev.unit_id.unwrap_or_else(|| {
                            warn!(
                                event_id = %ev.id,
                                source_id = %ev.source_id,
                                "missing unit_id while building kafka event; using nil UUID"
                            );
                            Uuid::nil()
                        }),
                    },
                    source_epoch: ev.source_epoch,
                    occurred_at: ev.occurred_at,
                    received_at: Utc::now(),
                    payload,
                };

                producer.produce_event(kafka_event).await;
            });
        }

        info!(
            kafka_produced = event_count,
            "enqueued events for kafka production"
        );
    }

    *buffered_events = 0;
}

// `fail_pending` removed: we no longer persist to DB, so marking failures
// for deferred DB writes is not applicable.
