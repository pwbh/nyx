use std::{collections::HashMap, fs, path::Path};

pub fn load_config(config_path: Option<String>) -> Result<HashMap<String, String>, String> {
    let mut config = HashMap::new();

    let content = if let Some(config_path) = config_path {
        config_path
    } else {
        // If no config path is provided we load the default config
        let default_config_path = Path::new("./config/dev.properties");
        let config_data = match fs::read_to_string(default_config_path) {
            Ok(c) => c,
            Err(e) => return Err(e.to_string()),
        };

        config_data
    };

    for (i, line) in content.lines().enumerate() {
        let mut split = line.split_terminator("=");
        let key = match split.next() {
            Some(k) => k,
            None => {
                return Err(format!(
                    "config loading error: key is missing on line {}",
                    i
                ))
            }
        };
        let value = match split.next() {
            Some(k) => k,
            None => {
                return Err(format!(
                    "config loading error: value is missing on line {}",
                    i
                ))
            }
        };
        config.insert(key.to_string(), value.to_string());
    }

    Ok(config)
}
