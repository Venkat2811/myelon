use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OracleMessage {
    pub sequence: u64,
    pub payload_hash: u64,
    pub payload_len: usize,
    pub timestamp_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleViolation {
    SequenceGap {
        consumer: usize,
        expected: u64,
        got: u64,
    },
    SequenceReorder {
        consumer: usize,
        prev: u64,
        got: u64,
    },
    PayloadMismatch {
        consumer: usize,
        sequence: u64,
        expected_hash: u64,
        got_hash: u64,
    },
    PayloadSizeMismatch {
        consumer: usize,
        sequence: u64,
        expected: usize,
        got: usize,
    },
    MissingMessages {
        consumer: usize,
        expected: u64,
        consumed: u64,
    },
    ExtraMessages {
        consumer: usize,
        expected: u64,
        consumed: u64,
    },
}

#[derive(Debug, Clone)]
pub struct MessageOracle {
    published: Vec<OracleMessage>,
    expected_per_consumer: Vec<Vec<OracleMessage>>,
}

impl MessageOracle {
    pub fn with_broadcast_consumers(consumers: usize) -> Self {
        Self {
            published: Vec::new(),
            expected_per_consumer: vec![Vec::new(); consumers],
        }
    }

    pub fn record_publish(&mut self, msg: OracleMessage) {
        self.published.push(msg.clone());
        for consumer in &mut self.expected_per_consumer {
            consumer.push(msg.clone());
        }
    }

    pub fn extend_published<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = OracleMessage>,
    {
        for msg in iter {
            self.record_publish(msg);
        }
    }

    pub fn published(&self) -> &[OracleMessage] {
        &self.published
    }

    pub fn expected_for_consumer(&self, consumer_id: usize) -> &[OracleMessage] {
        &self.expected_per_consumer[consumer_id]
    }

    pub fn verify_consumer_messages(
        &self,
        consumer_id: usize,
        actual: &[OracleMessage],
    ) -> Result<(), Vec<OracleViolation>> {
        let expected = self.expected_for_consumer(consumer_id);
        let mut violations = Vec::new();
        let mut previous = None::<u64>;

        for (index, actual_msg) in actual.iter().enumerate() {
            if let Some(prev) = previous {
                if actual_msg.sequence <= prev {
                    violations.push(OracleViolation::SequenceReorder {
                        consumer: consumer_id,
                        prev,
                        got: actual_msg.sequence,
                    });
                }
            }
            previous = Some(actual_msg.sequence);

            let Some(expected_msg) = expected.get(index) else {
                violations.push(OracleViolation::ExtraMessages {
                    consumer: consumer_id,
                    expected: expected.len() as u64,
                    consumed: actual.len() as u64,
                });
                break;
            };

            if actual_msg.sequence != expected_msg.sequence {
                violations.push(OracleViolation::SequenceGap {
                    consumer: consumer_id,
                    expected: expected_msg.sequence,
                    got: actual_msg.sequence,
                });
                continue;
            }

            if actual_msg.payload_hash != expected_msg.payload_hash {
                violations.push(OracleViolation::PayloadMismatch {
                    consumer: consumer_id,
                    sequence: actual_msg.sequence,
                    expected_hash: expected_msg.payload_hash,
                    got_hash: actual_msg.payload_hash,
                });
            }

            if actual_msg.payload_len != expected_msg.payload_len {
                violations.push(OracleViolation::PayloadSizeMismatch {
                    consumer: consumer_id,
                    sequence: actual_msg.sequence,
                    expected: expected_msg.payload_len,
                    got: actual_msg.payload_len,
                });
            }
        }

        if actual.len() < expected.len() {
            violations.push(OracleViolation::MissingMessages {
                consumer: consumer_id,
                expected: expected.len() as u64,
                consumed: actual.len() as u64,
            });
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    pub fn verify_complete(
        &self,
        reports: &[Vec<OracleMessage>],
    ) -> Result<(), Vec<OracleViolation>> {
        let mut violations = Vec::new();
        for (consumer_id, report) in reports.iter().enumerate() {
            if let Err(mut errs) = self.verify_consumer_messages(consumer_id, report) {
                violations.append(&mut errs);
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
}

pub fn payload_bytes(seed: u64, sequence: u64, payload_len: usize) -> Vec<u8> {
    let salt = (seed as u8).wrapping_mul(17);
    (0..payload_len)
        .map(|index| {
            (sequence as u8)
                .wrapping_add((index as u8).wrapping_mul(31))
                .wrapping_add(salt)
        })
        .collect()
}

pub fn stable_payload_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_detects_missing_messages() {
        let mut oracle = MessageOracle::with_broadcast_consumers(1);
        for seq in 0..3 {
            let payload = payload_bytes(1, seq, 4);
            oracle.record_publish(OracleMessage {
                sequence: seq,
                payload_hash: stable_payload_hash(&payload),
                payload_len: payload.len(),
                timestamp_ns: seq,
            });
        }

        let actual = vec![oracle.published()[0].clone(), oracle.published()[1].clone()];
        let errs = oracle.verify_complete(&[actual]).unwrap_err();
        assert!(matches!(
            errs.last(),
            Some(OracleViolation::MissingMessages {
                consumer: 0,
                expected: 3,
                consumed: 2
            })
        ));
    }
}
