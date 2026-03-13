use serde_json::Value;
use std::collections::HashSet;
use std::fs;

#[test]
fn c10_baseline_snapshot_has_required_fields() {
    let fixture_path = "tests/fixtures/c10_baseline_snapshot.json";
    let contents = fs::read_to_string(fixture_path).expect("fixture should be readable");
    let root: Value = serde_json::from_str(&contents).expect("fixture should be valid JSON");

    assert_root_fields(&root);
    assert_command_records(&root["commands"]);
    assert_test_tiers(
        &root["test_tiers"]["rust"],
        &["unit", "integration", "stress_like", "perf"],
    );
    assert_test_tiers(
        &root["test_tiers"]["python"],
        &["unit", "integration", "performance", "stress", "analysis"],
    );
    assert_benchmark_records(&root["benchmarks"]);
}

fn assert_root_fields(root: &Value) {
    let required = [
        "snapshot_id",
        "captured_at",
        "scope",
        "revision",
        "environment",
        "commands",
        "test_tiers",
        "benchmarks",
    ];

    for key in required {
        assert!(
            root.get(key).is_some(),
            "missing required root field '{key}'"
        );
    }

    let scope = &root["scope"];
    for key in ["primary_crate", "python_suite", "platform"] {
        assert!(scope.get(key).is_some(), "missing scope.{key}");
    }

    let revision = &root["revision"];
    for key in ["commit", "branch", "dirty"] {
        assert!(revision.get(key).is_some(), "missing revision.{key}");
    }

    let env = &root["environment"];
    for key in [
        "os",
        "kernel",
        "hostname",
        "architecture",
        "cpu",
        "rustc",
        "cargo",
    ] {
        assert!(env.get(key).is_some(), "missing environment.{key}");
    }
}

fn assert_command_records(commands: &Value) {
    let list = commands.as_array().expect("commands must be a JSON array");
    assert!(!list.is_empty(), "commands list must not be empty");

    let required_fields = ["name", "command", "exit_code", "status"];
    for (idx, command) in list.iter().enumerate() {
        let path = format!("commands[{idx}]");
        for field in required_fields {
            assert!(command.get(field).is_some(), "missing {path}.{field}");
        }
        assert!(
            command["status"].is_string(),
            "{path}.status must be a string"
        );
    }
}

fn assert_test_tiers(section: &Value, required_sections: &[&str]) {
    for section_name in required_sections {
        let tier = &section[*section_name];
        assert!(tier.is_object(), "{section_name} test tier must be object");
        let has_tests_count = tier.get("tests").and_then(Value::as_u64).is_some();
        let has_benchmarks_count = tier.get("benchmarks").and_then(Value::as_u64).is_some();
        assert!(
            has_tests_count || has_benchmarks_count,
            "{section_name} test tier must include numeric tests or benchmarks count"
        );

        if tier.get("tests").is_some() {
            assert!(
                tier["tests"].as_u64().is_some(),
                "{section_name}.tests must be a number"
            );
        }

        if tier.get("benchmarks").is_some() {
            assert!(
                tier["benchmarks"].as_u64().is_some(),
                "{section_name}benchmark-cache must be a number when present"
            );
        }

        if tier.get("tests_list").is_some() {
            assert!(
                tier["tests_list"].as_array().is_some(),
                "{section_name}.tests_list must be an array when present"
            );
        }
        if tier.get("bench_names").is_some() {
            assert!(
                tier["bench_names"].as_array().is_some(),
                "{section_name}.bench_names must be an array when present"
            );
        }
    }
}

fn assert_benchmark_records(benchmarks: &Value) {
    let records = benchmarks
        .as_array()
        .expect("benchmarks must be a JSON array");
    assert!(!records.is_empty(), "benchmarks list must not be empty");

    let required_top = [
        "benchmark_id",
        "command",
        "category",
        "executed",
        "status",
        "required_result_fields",
        "results",
    ];

    for (idx, bench) in records.iter().enumerate() {
        let path = format!("benchmarks[{idx}]");
        for field in required_top {
            assert!(bench.get(field).is_some(), "missing {path}.{field}");
        }

        assert!(
            bench["required_result_fields"].is_array(),
            "{path}.required_result_fields must be an array"
        );
        assert!(
            bench["results"].is_array(),
            "{path}.results must be an array"
        );
        assert!(
            bench["status"].is_string(),
            "{path}.status must be a string"
        );

        let required_fields = bench["required_result_fields"]
            .as_array()
            .expect("required_result_fields should be an array")
            .iter()
            .filter_map(|field| field.as_str())
            .collect::<HashSet<_>>();

        if bench["executed"].as_bool() == Some(true) {
            assert!(
                !required_fields.is_empty(),
                "{path} expected required_result_fields when executed"
            );
            for (row_idx, result) in bench["results"].as_array().unwrap().iter().enumerate() {
                assert!(
                    result.is_object(),
                    "{path}.results[{row_idx}] must be an object"
                );
                for required in &required_fields {
                    assert!(
                        result.get(required).is_some(),
                        "{path}.results[{row_idx}] missing required field '{required}'"
                    );
                }
            }
            assert!(
                !bench["results"].as_array().unwrap().is_empty(),
                "{path} executed benchmarks should include at least one result row"
            );
        }
    }
}
