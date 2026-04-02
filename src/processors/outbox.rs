use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

/// Minimal Outbox model prepared for a future Outbox Pattern
#[derive(Debug, Clone)]
pub struct OutboxEvent {
    pub id: Uuid,
    pub payload: Value,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[async_trait::async_trait]
pub trait OutboxRepository: Send + Sync {
    async fn save(&self, event: &OutboxEvent) -> anyhow::Result<()>;
}
