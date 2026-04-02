use anyhow::Result;
use dotenvy::dotenv;
use tokio;
use uuid::Uuid;

use event_processor::config::AppConfig;
use event_processor::processors::event_processor::Event;
use event_processor::processors::producer::ProducerService;
use event_processor::models::IncomingMessage;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // Load the app config from environment (.env)
    let config = AppConfig::load()?;

    let producer = ProducerService::new(&config.kafka)?;

    // Build a dummy incoming message
    let incoming = IncomingMessage {
        message_id: Some(Uuid::new_v4()),
        device_id: Some("device-123".to_string()),
        msg_class: "ALERT".to_string(),
        alert: Some("Test event".to_string()),
        fix_status: None,
        gps_epoch: None,
        received_at: None,
        source_id: None,
        unit_id: None,
        occurred_at: None,
        latitude: None,
        longitude: None,
        extra: Default::default(),
    };

    let event = Event::from_incoming(&incoming);

    producer.produce_event(event).await;

    Ok(())
}
