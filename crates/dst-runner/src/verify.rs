use crate::oracle::{MessageOracle, OracleViolation};
use crate::report::ChildReport;

pub fn verify_raw_ring_broadcast(
    oracle: &MessageOracle,
    consumers: &[ChildReport],
) -> Result<(), Vec<OracleViolation>> {
    let messages: Vec<Vec<_>> = consumers
        .iter()
        .map(|report| report.messages.clone())
        .collect();
    oracle.verify_complete(&messages)
}
