use myelon::inference::{FixedTopology, WorkerCount};
use std::error::Error;
use std::time::Duration;

#[derive(Copy, Clone, Default)]
struct InferenceEvent {
    token_id: u32,
    worker_id: u16,
    end_of_batch: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let topology = FixedTopology::new("infer_demo", 1024, WorkerCount::Three)
        .with_coordination_timeout(Duration::from_secs(5));
    let example_event = InferenceEvent::default();
    let _ = (
        example_event.token_id,
        example_event.worker_id,
        example_event.end_of_batch,
    );

    let _scheduler_builder = topology.scheduler_builder::<InferenceEvent>();
    let mut worker_builders = Vec::new();
    for worker_index in topology.worker_indices() {
        worker_builders.push(topology.worker_builder::<InferenceEvent>(worker_index)?);
    }
    let _ = worker_builders;

    Ok(())
}
