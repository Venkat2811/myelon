#[path = "../../../../disruptor-mp/benches/ipc/competitive/benchmark_pingpong.rs"]
mod battle_tested_competitive_pingpong;

fn main() {
    if let Err(error) = battle_tested_competitive_pingpong::bench_entrypoint() {
        eprintln!("competitive_shm failed: {error}");
        std::process::exit(1);
    }
}
