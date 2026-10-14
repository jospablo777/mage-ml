//! Avro messages in Confluent's wire format (a zero byte, the schema's id as a big-endian
//! u32, then the Avro body), decoded to the JSON blocks get, with each schema read once
//! from the schema registry (`GET /schemas/ids/{id}`).
//!
//! Values become JSON as Mage's blocks would read them: records objects, unions their value,
//! enums their symbol, bytes and fixed base64, decimals exact strings, dates, times and
//! timestamps ISO 8601 text (UTC timestamps end in Z), UUIDs text.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::schema::ResolvedSchema;
use apache_avro::types::Value as Avro;
use base64::Engine as _;
use serde_json::{Map, Number, Value};

/// Where the registry is and how to sign in.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

pub struct AvroDecoder {
    config: RegistryConfig,
    agent: ureq::Agent,
    schemas: HashMap<u32, Arc<Parsed>>,
}

/// A schema with its reader and named types, made once per schema id. Schemas are kept for
/// the source's life (a topic has few), so they are leaked to borrow them for good.
struct Parsed {
    schema: &'static Schema,
    reader: GenericDatumReader<'static>,
    names: ResolvedSchema<'static>,
}

impl Parsed {
    fn new(schema: Schema) -> Result<Parsed, String> {
        let schema: &'static Schema = Box::leak(Box::new(schema));
        Ok(Parsed {
            schema,
            reader: GenericDatumReader::builder(schema)
                .build()
                .map_err(|e| e.to_string())?,
            names: ResolvedSchema::try_from(schema).map_err(|e| e.to_string())?,
        })
    }
}

impl AvroDecoder {
    pub fn new(config: RegistryConfig) -> AvroDecoder {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(30)))
            .build()
            .into();
        AvroDecoder {
            config,
            agent,
            schemas: HashMap::new(),
        }
    }

    /// The message as JSON bytes; an error says why it could not be read.
    pub fn decode(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        if payload.len() < 5 || payload[0] != 0 {
            return Err(
                "the message is not in Confluent's Avro format (a zero byte and a \
                        schema id first)"
                    .into(),
            );
        }
        let id = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
        let parsed = self.schema(id)?;
        let mut body = &payload[5..];
        let value = parsed
            .reader
            .read_value(&mut body)
            .map_err(|e| format!("decoding with schema {id}: {e}"))?;
        let json = to_json(&value, parsed.schema, &parsed.names)?;
        serde_json::to_vec(&json).map_err(|e| e.to_string())
    }

    fn schema(&mut self, id: u32) -> Result<Arc<Parsed>, String> {
        if let Some(schema) = self.schemas.get(&id) {
            return Ok(schema.clone());
        }
        let url = format!("{}/schemas/ids/{id}", self.config.url.trim_end_matches('/'));
        let mut request = self
            .agent
            .get(&url)
            .header("Accept", "application/vnd.schemaregistry.v1+json");
        if let Some(username) = &self.config.username {
            let token = base64::engine::general_purpose::STANDARD.encode(format!(
                "{username}:{}",
                self.config.password.as_deref().unwrap_or("")
            ));
            request = request.header("Authorization", &format!("Basic {token}"));
        }
        let mut response = request
            .call()
            .map_err(|e| format!("reading schema {id} from the schema registry: {e}"))?;
        let body: Value = response
            .body_mut()
            .read_json()
            .map_err(|e| format!("the schema registry's answer for schema {id}: {e}"))?;
        if body["schemaType"].as_str().is_some_and(|t| t != "AVRO") {
            return Err(format!(
                "schema {id} is a {} schema; services read Avro",
                body["schemaType"].as_str().unwrap_or("?")
            ));
        }
        let text = body["schema"]
            .as_str()
            .ok_or_else(|| format!("the schema registry sent no schema for id {id}"))?;
        let schema = Schema::parse_str(text).map_err(|e| format!("schema {id}: {e}"))?;
        let parsed = Arc::new(Parsed::new(schema).map_err(|e| format!("schema {id}: {e}"))?);
        self.schemas.insert(id, parsed.clone());
        Ok(parsed)
    }
}

fn to_json(value: &Avro, schema: &Schema, names: &ResolvedSchema) -> Result<Value, String> {
    let schema = match schema {
        Schema::Ref { name } => *names
            .get_names()
            .get(name)
            .ok_or_else(|| format!("the schema names {name}, which it does not define"))?,
        other => other,
    };
    let base64 =
        |bytes: &[u8]| Value::String(base64::engine::general_purpose::STANDARD.encode(bytes));
    Ok(match value {
        Avro::Null => Value::Null,
        Avro::Boolean(b) => Value::Bool(*b),
        Avro::Int(n) => Value::from(*n),
        Avro::Long(n) => Value::from(*n),
        Avro::Float(x) => number(f64::from(*x))?,
        Avro::Double(x) => number(*x)?,
        Avro::Bytes(bytes) | Avro::Fixed(_, bytes) => base64(bytes),
        Avro::String(text) | Avro::Enum(_, text) => Value::String(text.clone()),
        Avro::Union(index, inner) => {
            let variant = match schema {
                Schema::Union(union) => union.variants().get(*index as usize),
                _ => None,
            }
            .ok_or("a union value does not match its schema")?;
            to_json(inner, variant, names)?
        }
        Avro::Array(items) => {
            let item_schema = match schema {
                Schema::Array(array) => &array.items,
                _ => return Err("an array value does not match its schema".into()),
            };
            Value::Array(
                items
                    .iter()
                    .map(|item| to_json(item, item_schema, names))
                    .collect::<Result<_, _>>()?,
            )
        }
        Avro::Map(entries) => {
            let value_schema = match schema {
                Schema::Map(map) => &map.types,
                _ => return Err("a map value does not match its schema".into()),
            };
            let mut object = Map::new();
            for (key, item) in entries {
                object.insert(key.clone(), to_json(item, value_schema, names)?);
            }
            Value::Object(object)
        }
        Avro::Record(fields) => {
            let field_schemas = match schema {
                Schema::Record(record) => &record.fields,
                _ => return Err("a record value does not match its schema".into()),
            };
            let mut object = Map::new();
            for ((name, item), field) in fields.iter().zip(field_schemas) {
                object.insert(name.clone(), to_json(item, &field.schema, names)?);
            }
            Value::Object(object)
        }
        Avro::Decimal(decimal) => {
            let scale = match schema {
                Schema::Decimal(decimal) => decimal.scale,
                _ => 0,
            };
            Value::String(scaled(
                num_bigint::BigInt::from(decimal.clone()).to_string(),
                scale,
            ))
        }
        Avro::BigDecimal(decimal) => Value::String(decimal.to_string()),
        Avro::Date(days) => Value::String(
            chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| {
                    epoch.checked_add_signed(chrono::TimeDelta::days(i64::from(*days)))
                })
                .ok_or("a date out of range")?
                .to_string(),
        ),
        Avro::TimeMillis(ms) => Value::String(time_of_day(i64::from(*ms) * 1_000_000)?),
        Avro::TimeMicros(us) => Value::String(time_of_day(us * 1_000)?),
        Avro::TimestampMillis(ms) => Value::String(timestamp(ms * 1_000_000, true)?),
        Avro::TimestampMicros(us) => Value::String(timestamp(us * 1_000, true)?),
        Avro::TimestampNanos(ns) => Value::String(timestamp(*ns, true)?),
        Avro::LocalTimestampMillis(ms) => Value::String(timestamp(ms * 1_000_000, false)?),
        Avro::LocalTimestampMicros(us) => Value::String(timestamp(us * 1_000, false)?),
        Avro::LocalTimestampNanos(ns) => Value::String(timestamp(*ns, false)?),
        Avro::Duration(duration) => {
            let mut object = Map::new();
            object.insert("months".into(), Value::from(u32::from(duration.months())));
            object.insert("days".into(), Value::from(u32::from(duration.days())));
            object.insert("millis".into(), Value::from(u32::from(duration.millis())));
            Value::Object(object)
        }
        Avro::Uuid(uuid) => Value::String(uuid.to_string()),
    })
}

/// NaN and infinity have no JSON form: refused, as in Python blocks' messages.
fn number(x: f64) -> Result<Value, String> {
    Number::from_f64(x)
        .map(Value::Number)
        .ok_or_else(|| format!("{x} has no JSON form"))
}

/// The unscaled digits with the decimal point `scale` places from the right.
fn scaled(digits: String, scale: usize) -> String {
    if scale == 0 {
        return digits;
    }
    let (sign, digits) = match digits.strip_prefix('-') {
        Some(rest) => ("-", rest.to_string()),
        None => ("", digits),
    };
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = padded.split_at(padded.len() - scale);
    format!("{sign}{whole}.{fraction}")
}

fn time_of_day(nanos: i64) -> Result<String, String> {
    let seconds = u32::try_from(nanos / 1_000_000_000).map_err(|_| "a time out of range")?;
    let fraction = u32::try_from(nanos % 1_000_000_000).map_err(|_| "a time out of range")?;
    chrono::NaiveTime::from_num_seconds_from_midnight_opt(seconds, fraction)
        .map(|t| t.format("%H:%M:%S%.f").to_string())
        .ok_or_else(|| "a time out of range".into())
}

fn timestamp(nanos: i64, utc: bool) -> Result<String, String> {
    let at = chrono::DateTime::from_timestamp_nanos(nanos);
    Ok(if utc {
        at.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
    } else {
        at.naive_utc().format("%Y-%m-%dT%H:%M:%S%.f").to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::writer::datum::GenericDatumWriter;

    fn framed(schema: &Schema, value: Avro, id: u32) -> Vec<u8> {
        let mut bytes = vec![0];
        bytes.extend_from_slice(&id.to_be_bytes());
        let writer = GenericDatumWriter::builder(schema).build().unwrap();
        bytes.extend(writer.write_value_to_vec(value).unwrap());
        bytes
    }

    fn decoder_with(id: u32, schema: Schema) -> AvroDecoder {
        let mut decoder = AvroDecoder::new(RegistryConfig {
            url: "http://127.0.0.1:1".into(),
            username: None,
            password: None,
        });
        decoder
            .schemas
            .insert(id, Arc::new(Parsed::new(schema).unwrap()));
        decoder
    }

    #[test]
    fn records_unions_logical_types_and_decimals_become_exact_json() {
        let schema = Schema::parse_str(
            r#"{"type": "record", "name": "Order", "namespace": "shop", "fields": [
                {"name": "id", "type": "long"},
                {"name": "note", "type": ["null", "string"]},
                {"name": "price", "type": {"type": "bytes", "logicalType": "decimal",
                                           "precision": 30, "scale": 2}},
                {"name": "day", "type": {"type": "int", "logicalType": "date"}},
                {"name": "at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "items", "type": {"type": "array", "items": {"type": "record",
                    "name": "Item", "fields": [{"name": "sku", "type": "string"}]}}},
                {"name": "again", "type": ["null", "Item"]},
                {"name": "state", "type": {"type": "enum", "name": "State",
                                           "symbols": ["NEW", "PAID"]}}
            ]}"#,
        )
        .unwrap();
        let item = || Avro::Record(vec![("sku".into(), Avro::String("a-1".into()))]);
        let value = Avro::Record(vec![
            ("id".into(), Avro::Long(9_007_199_254_740_993)),
            (
                "note".into(),
                Avro::Union(1, Box::new(Avro::String("Zürich 🚀".into()))),
            ),
            (
                "price".into(),
                Avro::Decimal(apache_avro::Decimal::from(
                    num_bigint::BigInt::from(-12_345_678_901_234_567_i64).to_signed_bytes_be(),
                )),
            ),
            ("day".into(), Avro::Date(20_740)),
            ("at".into(), Avro::TimestampMillis(1_792_000_000_123)),
            ("items".into(), Avro::Array(vec![item()])),
            ("again".into(), Avro::Union(1, Box::new(item()))),
            ("state".into(), Avro::Enum(1, "PAID".into())),
        ]);
        let mut decoder = decoder_with(7, schema.clone());

        let json: Value =
            serde_json::from_slice(&decoder.decode(&framed(&schema, value, 7)).unwrap()).unwrap();

        assert_eq!(json["id"], serde_json::json!(9_007_199_254_740_993_i64));
        assert_eq!(json["note"], "Zürich 🚀");
        assert_eq!(json["price"], "-123456789012345.67");
        assert_eq!(json["day"], "2026-10-14");
        assert_eq!(json["at"], "2026-10-14T17:46:40.123Z");
        assert_eq!(json["items"][0]["sku"], "a-1");
        assert_eq!(json["again"]["sku"], "a-1");
        assert_eq!(json["state"], "PAID");
    }

    #[test]
    fn a_message_without_the_wire_format_is_refused_with_why() {
        let mut decoder = decoder_with(1, Schema::Long);
        let error = decoder.decode(b"{\"json\": true}").unwrap_err();
        assert!(error.contains("Confluent's Avro format"), "{error}");
    }

    #[test]
    fn small_decimals_keep_their_leading_zeros() {
        assert_eq!(scaled("5".into(), 2), "0.05");
        assert_eq!(scaled("-5".into(), 3), "-0.005");
        assert_eq!(scaled("12345".into(), 0), "12345");
    }
}
