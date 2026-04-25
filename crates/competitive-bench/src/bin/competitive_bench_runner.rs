use clap::Parser;
use competitive_bench::runner::cli::RunnerCli;
use competitive_bench::runner::executor::execute_tier;

fn main() {
    let cli = RunnerCli::parse();

    eprintln!(
        "=== competitive-bench-runner: tier={} adapters={} outdir={} ===",
        cli.tier,
        cli.adapters,
        cli.outdir.display()
    );

    let outcomes = execute_tier(&cli);

    let total = outcomes.len();
    let passed = outcomes.iter().filter(|o| o.success).count();
    let failed = total - passed;

    eprintln!();
    eprintln!("=== {passed}/{total} passed, {failed} failed ===");

    if failed > 0 {
        for outcome in &outcomes {
            if !outcome.success {
                eprintln!("  FAIL: {} {}", outcome.display_name, outcome.mode);
            }
        }
        std::process::exit(1);
    }
}
