use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::config::PostgresConfig;
use crate::models::EventTypeRegistry;

pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn connect(config: &PostgresConfig) -> Result<Self, sqlx::Error> {
        let connection_string = format!(
            "postgres://{}:{}@{}:{}/{}",
            config.user, config.password, config.host, config.port, config.db_name
        );

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&connection_string)
            .await?;

        Ok(Self { pool })
    }

    // `insert_events` removed: events are no longer persisted to the DB.

    pub async fn health_check(&self) -> bool {
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .is_ok()
    }

    pub async fn load_event_types(&self) -> Result<EventTypeRegistry, sqlx::Error> {
        let rows = sqlx::query("SELECT id, code FROM event_types")
            .fetch_all(&self.pool)
            .await?;

        let registry = rows
            .into_iter()
            .map(|r| {
                let code: String = r.get("code");
                let id: Uuid = r.get("id");
                (code, id)
            })
            .collect();

        Ok(registry)
    }

    pub async fn load_unit_devices(
        &self,
    ) -> Result<std::collections::HashMap<String, Uuid>, sqlx::Error> {
        let rows = sqlx::query("SELECT unit_id, device_id FROM unit_devices")
            .fetch_all(&self.pool)
            .await?;

        let registry = rows
            .into_iter()
            .map(|r| {
                let device_id: String = r.get("device_id");
                let unit_id: Uuid = r.get("unit_id");
                (device_id, unit_id)
            })
            .collect();

        Ok(registry)
    }

    pub async fn find_unit_id_by_device(
        &self,
        device_id: &str,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        let row = sqlx::query("SELECT unit_id, device_id FROM unit_devices WHERE device_id = $1")
            .bind(device_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|r| r.get("unit_id")))
    }
}
