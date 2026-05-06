use std::collections::HashMap;

/// Reference benchmark data from shmipc-rs and shmipc-go
pub struct CompetitorBenchmarks {
    pub shmipc_rs: HashMap<usize, f64>,
    pub shmipc_go: HashMap<usize, f64>,
}

impl CompetitorBenchmarks {
    pub fn new() -> Self {
        let mut shmipc_rs = HashMap::new();
        let mut shmipc_go = HashMap::new();

        // shmipc-rs benchmark results (in microseconds)
        shmipc_rs.insert(64, 1.066);
        shmipc_rs.insert(512, 1.013);
        shmipc_rs.insert(1024, 1.051);
        shmipc_rs.insert(4096, 1.018);
        shmipc_rs.insert(16384, 1.226);
        shmipc_rs.insert(32768, 1.184);
        shmipc_rs.insert(65536, 1.254);
        shmipc_rs.insert(262144, 1.167);
        shmipc_rs.insert(524288, 1.260);
        shmipc_rs.insert(1048576, 2.387);
        shmipc_rs.insert(4194304, 4.818);

        // shmipc-go benchmark results (in microseconds)
        shmipc_go.insert(64, 1.970);
        shmipc_go.insert(512, 1.990);
        shmipc_go.insert(1024, 2.045);
        shmipc_go.insert(4096, 2.063);
        shmipc_go.insert(16384, 1.996);
        shmipc_go.insert(32768, 1.937);
        shmipc_go.insert(65536, 1.995);
        shmipc_go.insert(262144, 1.793);
        shmipc_go.insert(524288, 1.993);
        shmipc_go.insert(1048576, 1.873);
        shmipc_go.insert(4194304, 1.891);

        Self {
            shmipc_rs,
            shmipc_go,
        }
    }

    pub fn get_speedup(
        &self,
        size: usize,
        our_latency_ns: f64,
        competitor: &str,
    ) -> Option<String> {
        let our_latency_us = our_latency_ns / 1000.0;

        let competitor_map = match competitor {
            "shmipc-rs" => &self.shmipc_rs,
            "shmipc-go" => &self.shmipc_go,
            _ => return None,
        };

        if let Some(&competitor_latency) = competitor_map.get(&size) {
            let speedup = competitor_latency / our_latency_us;

            if speedup >= 1.0 {
                Some(format!("{:.1}x faster 🟢", speedup))
            } else {
                Some(format!("{:.1}x slower 🔴", 1.0 / speedup))
            }
        } else {
            Some("N/A".to_string())
        }
    }
}
