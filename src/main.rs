mod infrai_queue;
mod legal_dead_letter;

use infrai_queue::QueueClient;
use legal_dead_letter::{decide, Job, JobKind, JobOutcome};
use serde_json::json;

fn main() -> Result<(), String> {
    let client = QueueClient::from_env().map_err(|e| format!("client setup: {e:?}"))?;
    let queue = format!("legal-jobs-{}", std::process::id());
    client
        .create(&queue)
        .map_err(|e| format!("create: {e:?}"))?;
    let dead_letter_queue = format!("{queue}-dead-letter");
    if let Err(error) = client.create(&dead_letter_queue) {
        let cleanup = client.delete(&queue);
        return Err(format!(
            "create dead-letter queue: {error:?}; cleanup: {cleanup:?}"
        ));
    }
    let result = run_worker(&client, &queue, &dead_letter_queue);
    let dlq_cleanup = client.delete(&dead_letter_queue);
    let cleanup = client.delete(&queue);
    if let Err(error) = cleanup {
        return Err(format!("{result:?}; cleanup failed: {error:?}"));
    }
    dlq_cleanup.map_err(|e| format!("{result:?}; dead-letter cleanup failed: {e:?}"))?;
    println!("deleted queues: {queue}, {dead_letter_queue}");
    result
}

fn run_worker(client: &QueueClient, queue: &str, dead_letter_queue: &str) -> Result<(), String> {
    let job = Job {
        matter_id: "matter-104".to_string(),
        kind: JobKind::SignedDocumentDelivery,
        attempts: 3,
    };
    client.publish(queue, &json!({"matter_id": job.matter_id, "attempts": job.attempts, "kind": "signed_document_delivery"})).map_err(|e| format!("publish: {e:?}"))?;
    let consumed = client
        .consume(queue)
        .map_err(|e| format!("consume: {e:?}"))?;
    let messages = consumed
        .get("items")
        .and_then(|items| items.as_array())
        .ok_or_else(|| "consume response did not contain a message array".to_string())?;
    for message in messages {
        let id = message
            .get("message_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "consumed message has no message_id".to_string())?;
        let payload = message
            .get("payload")
            .ok_or_else(|| "consumed message has no payload".to_string())?;
        let job: Job = if let Some(encoded) = payload.as_str() {
            serde_json::from_str(encoded)
        } else {
            serde_json::from_value(payload.clone())
        }
        .map_err(|e| format!("decode job: {e}"))?;
        match decide(&job) {
            JobOutcome::DeadLetter { reason } => {
                client
                    .publish(
                        dead_letter_queue,
                        &serde_json::to_value(&job).map_err(|e| e.to_string())?,
                    )
                    .map_err(|e| format!("dead-letter publish: {e:?}"))?;
                client.ack(queue, id).map_err(|e| format!("ack: {e:?}"))?;
                println!("{} -> dead letter: {}", job.matter_id, reason);
            }
            JobOutcome::Retry => {
                client
                    .nack(queue, id, true)
                    .map_err(|e| format!("nack: {e:?}"))?;
                println!("{} -> retry", job.matter_id);
            }
            JobOutcome::Completed => {
                client.ack(queue, id).map_err(|e| format!("ack: {e:?}"))?;
                println!("{} -> completed", job.matter_id);
            }
        }
    }
    Ok(())
}
