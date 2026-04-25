fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    competitive_bench::adapters::zmq::pingpong::main()
}
