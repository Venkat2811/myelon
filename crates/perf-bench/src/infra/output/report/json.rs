use super::model::ReportBundle;

impl ReportBundle {
    pub fn to_json_pretty(&self) -> String {
        serde_json::to_string_pretty(self).expect("serialize report v2")
    }

    pub fn write_json(&self, path: &str) -> std::io::Result<()> {
        std::fs::write(path, self.to_json_pretty())
    }
}
