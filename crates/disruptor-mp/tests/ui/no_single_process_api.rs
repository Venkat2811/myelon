use disruptor_mp::{build_multi_producer, build_single_producer, BusySpin};

fn main() {
    let _ = build_single_producer::<_, _>(64, || 0u64, BusySpin);
    let _ = build_multi_producer::<_, _>(64, || 0u64, BusySpin);
}
