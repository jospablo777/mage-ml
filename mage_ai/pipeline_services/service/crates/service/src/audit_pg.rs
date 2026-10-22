//! The audit store on PostgreSQL: MAGE_SERVICE_AUDIT_URL `postgres://...`. Each record of
//! a copied piece of a file (audit_ship.rs) is a row of MAGE_SERVICE_AUDIT_TABLE
//! (`mage_audit_records`), keyed by service, instance, model, file and line number, so a
//! copy repeated after a crash adds nothing. `record` holds the line as written, so a
//! file's chain checks on the rows in line order; `record::jsonb` queries its fields.

use std::sync::mpsc;

type Job = Box<dyn FnOnce(&tokio::runtime::Runtime, &mut Option<tokio_postgres::Client>) + Send>;

/// Rows per INSERT.
const ROWS_PER_STATEMENT: usize = 1000;

pub struct PgStore {
    jobs: std::sync::Mutex<mpsc::Sender<Job>>,
    url: String,
    table: String,
}

fn quote(name: &str) -> String {
    name.split('.')
        .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

impl PgStore {
    /// Connects now and creates the table, so a wrong URL stops the service at start.
    pub fn connect(url: &str, table: &str) -> Result<PgStore, String> {
        let (jobs, receive) = mpsc::channel::<Job>();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("audit store runtime: {e}"))?;
        // The client lives on its own thread: blocking on it from a thread inside another
        // runtime would panic.
        std::thread::Builder::new()
            .name("audit-postgres".into())
            .spawn(move || {
                let mut client = None;
                while let Ok(job) = receive.recv() {
                    job(&runtime, &mut client);
                }
            })
            .map_err(|e| format!("audit store thread: {e}"))?;
        let store = PgStore {
            jobs: std::sync::Mutex::new(jobs),
            url: url.to_string(),
            table: quote(table),
        };
        let table = store.table.clone();
        let index = quote(&format!("{}_key", table.replace('"', "").replace('.', "_")));
        store.run(move |runtime, client| {
            runtime
                .block_on(client.batch_execute(&format!(
                    "CREATE TABLE IF NOT EXISTS {table} (
                        service text NOT NULL,
                        instance text NOT NULL,
                        model text NOT NULL,
                        file text NOT NULL,
                        line bigint NOT NULL,
                        key text,
                        record text NOT NULL,
                        copied_at timestamptz NOT NULL DEFAULT now(),
                        PRIMARY KEY (service, instance, model, file, line)
                    );
                    CREATE INDEX IF NOT EXISTS {index} ON {table} (model, key)"
                )))
                .map_err(|e| format!("creating the audit table {table}: {e}"))
        })?;
        Ok(store)
    }

    fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&tokio::runtime::Runtime, &tokio_postgres::Client) -> Result<T, String>
        + Send
        + 'static,
    ) -> Result<T, String> {
        let url = self.url.clone();
        let (reply, answer) = mpsc::channel();
        self.jobs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .send(Box::new(move |runtime, client| {
                // Connects when there is no connection or it closed.
                if client.as_ref().is_none_or(|c| c.is_closed()) {
                    match runtime.block_on(mage_pg::connect_url(&url, "mage-service-audit")) {
                        Ok(connected) => *client = Some(connected),
                        Err(error) => {
                            let _ = reply.send(Err(error));
                            return;
                        }
                    }
                }
                let _ = reply.send(work(runtime, client.as_ref().expect("connected")));
            }))
            .map_err(|_| "the audit store stopped".to_string())?;
        answer
            .recv()
            .map_err(|_| "the audit store stopped".to_string())?
    }
}

impl crate::audit_ship::Store for PgStore {
    /// `key` is `<service>/<instance>/<model>/<file>/<first line>.jsonl`.
    fn put(&self, key: &str, bytes: Vec<u8>) -> Result<(), String> {
        let parts: Vec<&str> = key.rsplitn(5, '/').collect();
        let [last, file, model, instance, service] = parts[..] else {
            return Err(format!("unexpected audit object name {key}"));
        };
        let first: i64 = last
            .trim_end_matches(".jsonl")
            .parse()
            .map_err(|_| format!("unexpected audit object name {key}"))?;
        let text = String::from_utf8(bytes).map_err(|e| format!("{key}: {e}"))?;
        let mut lines = Vec::new();
        let mut keys = Vec::new();
        let mut records = Vec::new();
        for (offset, line) in text.lines().enumerate() {
            lines.push(first + offset as i64);
            keys.push(
                serde_json::from_str::<serde_json::Value>(line)
                    .ok()
                    .and_then(|record| match record.get("key") {
                        Some(serde_json::Value::String(text)) => Some(text.clone()),
                        Some(serde_json::Value::Null) | None => None,
                        Some(other) => Some(other.to_string()),
                    }),
            );
            records.push(line.to_string());
        }
        let (service, instance, model, file) = (
            service.to_string(),
            instance.to_string(),
            model.to_string(),
            file.to_string(),
        );
        let sql = format!(
            "INSERT INTO {} (service, instance, model, file, line, key, record)
             SELECT $1, $2, $3, $4, line, key, record
             FROM unnest($5::bigint[], $6::text[], $7::text[]) AS r(line, key, record)
             ON CONFLICT DO NOTHING",
            self.table
        );
        self.run(move |runtime, client| {
            for start in (0..lines.len()).step_by(ROWS_PER_STATEMENT) {
                let end = (start + ROWS_PER_STATEMENT).min(lines.len());
                runtime
                    .block_on(client.execute(
                        &sql,
                        &[
                            &service,
                            &instance,
                            &model,
                            &file,
                            &lines[start..end].to_vec(),
                            &keys[start..end].to_vec(),
                            &records[start..end].to_vec(),
                        ],
                    ))
                    .map_err(|e| format!("writing audit records of {model}/{file}: {e}"))?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit_ship::Store;

    fn url() -> Option<String> {
        let url = std::env::var("MAGE_TEST_POSTGRES_URL").ok();
        if url.is_none() {
            eprintln!("MAGE_TEST_POSTGRES_URL is not set: skipped");
        }
        url
    }

    #[test]
    fn records_become_rows_once_and_keep_their_text() {
        let Some(url) = url() else { return };
        let table = format!(
            "audit_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let store = PgStore::connect(&url, &table).unwrap();
        let first = "{\"key\":42,\"previous\":null}\n{\"key\":\"a b\",\"previous\":\"x\"}\n";
        store
            .put(
                "scores/pod-1/churn/2026-10-22-41/000000000000.jsonl",
                first.into(),
            )
            .unwrap();
        // Again, as after a crash before the copy record was saved, and the next piece.
        store
            .put(
                "scores/pod-1/churn/2026-10-22-41/000000000000.jsonl",
                first.into(),
            )
            .unwrap();
        store
            .put(
                "scores/pod-1/churn/2026-10-22-41/000000000002.jsonl",
                "{\"key\": 7}\n".into(),
            )
            .unwrap();

        let quoted = quote(&table);
        let rows = store
            .run(move |runtime, client| {
                let rows = runtime
                    .block_on(client.query(
                        &format!("SELECT line, key, record FROM {quoted} ORDER BY line"),
                        &[],
                    ))
                    .map_err(|e| e.to_string())?;
                runtime
                    .block_on(client.batch_execute(&format!("DROP TABLE {quoted}")))
                    .map_err(|e| e.to_string())?;
                Ok(rows
                    .iter()
                    .map(|r| {
                        (
                            r.get::<_, i64>(0),
                            r.get::<_, Option<String>>(1),
                            r.get::<_, String>(2),
                        )
                    })
                    .collect::<Vec<_>>())
            })
            .unwrap();
        assert_eq!(
            rows,
            [
                (
                    0,
                    Some("42".into()),
                    "{\"key\":42,\"previous\":null}".into()
                ),
                (
                    1,
                    Some("a b".into()),
                    "{\"key\":\"a b\",\"previous\":\"x\"}".into()
                ),
                (2, Some("7".into()), "{\"key\": 7}".into()),
            ]
        );
    }

    #[test]
    fn a_wrong_url_fails_at_start() {
        let error = PgStore::connect("postgres://nobody@127.0.0.1:1/none", "t").err();
        assert!(error.is_some());
    }
}
