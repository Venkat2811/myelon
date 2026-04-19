fn main() -> Result<(), Box<dyn std::error::Error>> {
    perf_bench::executor_v2::disruptor_mp::mmap::competitive::run_main()
}
