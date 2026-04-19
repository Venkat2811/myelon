fn main() -> Result<(), Box<dyn std::error::Error>> {
    perf_bench::executor_v2::disruptor_mp::shm::competitive::run_main()
}
