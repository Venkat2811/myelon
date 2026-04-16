#[path = "../../disruptor-mp/benches/ipc/benchmark_all_wait_strategies_auto.rs"]
mod battle_tested_wait_strategy_auto;

fn main() {
    if let Err(error) = battle_tested_wait_strategy_auto::bench_entrypoint() {
        eprintln!("wait_strategy_shm failed: {error}");
        std::process::exit(1);
    }
}
