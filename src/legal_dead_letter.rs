use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    MatterIntake,
    SignedDocumentDelivery,
    DeadlineFollowUp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub matter_id: String,
    pub kind: JobKind,
    pub attempts: u8,
}

#[derive(Debug, PartialEq, Eq)]
pub enum JobOutcome {
    Retry,
    DeadLetter { reason: &'static str },
    Completed,
}

pub fn decide(job: &Job) -> JobOutcome {
    if job.attempts >= 3 {
        return JobOutcome::DeadLetter {
            reason: "retry limit reached",
        };
    }
    match job.kind {
        JobKind::MatterIntake | JobKind::SignedDocumentDelivery | JobKind::DeadlineFollowUp => {
            JobOutcome::Retry
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poison_signed_delivery_moves_to_dead_letter_after_three_attempts() {
        let job = Job {
            matter_id: "matter-104".into(),
            kind: JobKind::SignedDocumentDelivery,
            attempts: 3,
        };
        assert_eq!(
            decide(&job),
            JobOutcome::DeadLetter {
                reason: "retry limit reached"
            }
        );
    }
}
