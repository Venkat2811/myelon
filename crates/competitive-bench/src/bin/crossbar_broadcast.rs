#[path = "../crossbar_broadcast_impl.rs"]
mod crossbar_broadcast_impl;

use crossbar_broadcast_impl::{consumer, controller, parse_args, validate_args};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    validate_args(&args)?;
    if args.mode == "controller" {
        controller(args)
    } else {
        consumer(args)
    }
}
