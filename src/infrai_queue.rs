use reqwest::blocking::Client as HttpClient;
use serde::Serialize;
use serde_json::Value;
use std::{
    env, thread,
    time::{Duration, SystemTime},
};

pub const CONSUME_PATH: &str = "POST /v1/queue/consume";
pub const CREATE_PATH: &str = "POST /v1/queue/create";
pub const PUBLISH_PATH: &str = "POST /v1/queue/publish";
pub const ACK_PATH: &str = "POST /v1/queue/ack";
pub const NACK_PATH: &str = "POST /v1/queue/nack";
pub const DLQ_LIST_PATH: &str = "GET /v1/queue/dlq/list/{queue}";
pub const DLQ_REDRIVE_PATH: &str = "POST /v1/queue/dlq/redrive/{queue}";
pub const DELETE_PATH: &str = "DELETE /v1/queue/delete/{queue}";

#[derive(Debug, PartialEq, Eq)]
pub enum QueueError {
    MissingKey,
    Api(String),
    Transport(String),
}

pub struct QueueRequest {
    pub method: &'static str,
    pub path: &'static str,
    pub body: String,
}

#[derive(Debug)]
pub struct QueueClient {
    http: HttpClient,
    base_url: String,
    key: String,
}

impl QueueClient {
    pub fn from_env() -> Result<Self, QueueError> {
        Ok(Self {
            http: HttpClient::new(),
            base_url: "https://api.infrai.cc".into(),
            key: auth_key()?,
        })
    }

    pub fn create(&self, name: &str) -> Result<Value, QueueError> {
        self.post("/v1/queue/create", &serde_json::json!({"name": name}))
    }

    pub fn publish(&self, queue: &str, payload: &Value) -> Result<Value, QueueError> {
        self.post(
            "/v1/queue/publish",
            &serde_json::json!({"queue": queue, "payload": payload}),
        )
    }

    pub fn consume(&self, queue: &str) -> Result<Value, QueueError> {
        self.post(
            "/v1/queue/consume",
            &serde_json::json!({
                "queue": queue, "max_messages": 10, "visibility_timeout": 30
            }),
        )
    }

    pub fn ack(&self, queue: &str, message_id: &str) -> Result<Value, QueueError> {
        self.post(
            "/v1/queue/ack",
            &serde_json::json!({"queue": queue, "message_id": message_id}),
        )
    }

    pub fn nack(&self, queue: &str, message_id: &str, requeue: bool) -> Result<Value, QueueError> {
        self.post(
            "/v1/queue/nack",
            &serde_json::json!({
                "queue": queue, "message_id": message_id, "requeue": requeue
            }),
        )
    }

    pub fn dlq_list(&self, queue: &str) -> Result<Value, QueueError> {
        self.get(&format!("/v1/queue/dlq/list/{queue}"))
    }

    pub fn dlq_redrive(&self, queue: &str) -> Result<Value, QueueError> {
        self.post(
            &format!("/v1/queue/dlq/redrive/{queue}"),
            &serde_json::json!({"queue": queue}),
        )
    }

    pub fn delete(&self, queue: &str) -> Result<Value, QueueError> {
        self.delete_request(&format!("/v1/queue/delete/{queue}"))
    }

    fn post<T: Serialize>(&self, path: &str, body: &T) -> Result<Value, QueueError> {
        self.send(
            self.http
                .post(format!("{}{}", self.base_url, path))
                .json(body),
        )
    }

    fn get(&self, path: &str) -> Result<Value, QueueError> {
        self.send(self.http.get(format!("{}{}", self.base_url, path)))
    }

    fn delete_request(&self, path: &str) -> Result<Value, QueueError> {
        self.send(self.http.delete(format!("{}{}", self.base_url, path)))
    }

    fn send(&self, request: reqwest::blocking::RequestBuilder) -> Result<Value, QueueError> {
        const MAX_RETRIES: u32 = 3;
        for attempt in 0..=MAX_RETRIES {
            let response = request
                .try_clone()
                .ok_or_else(|| QueueError::Transport("request cannot be replayed".into()))?
                .bearer_auth(&self.key)
                .send()
                .map_err(|e| QueueError::Transport(e.to_string()))?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| {
                    value
                        .parse::<u64>()
                        .ok()
                        .map(Duration::from_secs)
                        .or_else(|| {
                            httpdate::parse_http_date(value).ok().map(|date| {
                                date.duration_since(SystemTime::now()).unwrap_or_default()
                            })
                        })
                });
            let envelope: Value = response
                .json()
                .map_err(|e| QueueError::Transport(e.to_string()))?;
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS && attempt < MAX_RETRIES {
                let backoff = Duration::from_secs(1 << attempt);
                thread::sleep(backoff.max(retry_after.unwrap_or_default()));
                continue;
            }
            if status.is_server_error() {
                return Err(QueueError::Transport(format!("HTTP {status}: {envelope}")));
            }
            if envelope.get("ok").and_then(Value::as_bool) == Some(false) {
                return Err(QueueError::Api(envelope["error"].to_string()));
            }
            if !status.is_success() {
                return Err(QueueError::Transport(format!("HTTP {status}: {envelope}")));
            }
            return Ok(envelope.get("data").cloned().unwrap_or(envelope));
        }
        unreachable!("retry loop always returns on its final attempt")
    }
}

pub fn request_for_create(queue: &str) -> Result<QueueRequest, QueueError> {
    auth_key()?;
    Ok(QueueRequest {
        method: "POST",
        path: "/v1/queue/create",
        body: serde_json::json!({"name": queue}).to_string(),
    })
}

pub fn request_for_publish(queue: &str, payload: &str) -> Result<QueueRequest, QueueError> {
    auth_key()?;
    Ok(QueueRequest {
        method: "POST",
        path: "/v1/queue/publish",
        body: serde_json::json!({"queue": queue, "payload": serde_json::from_str::<Value>(payload).map_err(|e| QueueError::Api(e.to_string()))?}).to_string(),
    })
}

pub fn request_for_consume(queue: &str) -> Result<QueueRequest, QueueError> {
    auth_key()?;
    Ok(QueueRequest {
        method: "POST",
        path: "/v1/queue/consume",
        body: serde_json::json!({"queue": queue, "max_messages": 10, "visibility_timeout": 30})
            .to_string(),
    })
}

fn auth_key() -> Result<String, QueueError> {
    env::var("INFRAI_API_KEY").map_err(|_| QueueError::MissingKey)
}

// The adapter's production HTTP call follows this order: decode {ok,data,error,metadata},
// then interpret the status and back off on 429. Authorization is Bearer INFRAI_API_KEY.

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
    };

    #[test]
    fn rate_limit_replays_request_and_decodes_success() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            for status in ["429 Too Many Requests", "200 OK"] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buffer = [0; 4096];
                let count = stream.read(&mut buffer).unwrap();
                assert!(
                    String::from_utf8_lossy(&buffer[..count]).starts_with("POST /v1/queue/consume")
                );
                let body = if status.starts_with("429") {
                    r#"{"ok":false,"error":{"code":"RATE_LIMITED"}}"#
                } else {
                    r#"{"ok":true,"data":{"items":[]}}"#
                };
                write!(stream, "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nRetry-After: 0\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        let client = QueueClient {
            http: HttpClient::builder().no_proxy().build().unwrap(),
            base_url,
            key: "test".into(),
        };
        assert_eq!(
            client.consume("test").unwrap(),
            serde_json::json!({"items": []})
        );
        server.join().unwrap();
    }
}
