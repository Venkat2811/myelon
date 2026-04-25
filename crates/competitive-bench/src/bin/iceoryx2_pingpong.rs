#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    competitive_bench::adapters::iceoryx2::pingpong::main()
}

#[cfg(not(unix))]
fn main() {
    competitive_bench::adapters::iceoryx2::pingpong::main()
}
