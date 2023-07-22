use std::{collections::HashMap, fs, path::PathBuf};

#[derive(Debug)]
pub enum Value {
    String(String),
    Number(i32),
    Float(f32),
}

#[derive(Debug)]
pub struct Config {
    inner: HashMap<String, Value>,
}

impl Config {
    pub fn from(path: PathBuf) -> Result<Self, String> {
        let mut inner = HashMap::new();

        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => return Err(e.to_string()),
        };

        for line in content.lines() {
            if line.starts_with('#') {
                continue;
            }

            let split: Vec<&str> = line.split_terminator('=').collect();

            if split.len() != 2 {
                return Err("property format is incorrect, should be key=value".to_string());
            }

            let key = split[0].to_string();
            let value = if let Ok(f) = split[1].parse::<f32>() {
                if split[1].contains('.') {
                    Value::Float(f)
                } else {
                    Value::Number(split[1].parse::<i32>().unwrap())
                }
            } else if let Ok(i) = split[1].parse::<i32>() {
                Value::Number(i)
            } else {
                Value::String(split[1].to_string())
            };

            inner.entry(key).or_insert(value);
        }

        Ok(Self { inner })
    }

    pub fn get(&self, k: &str) -> Option<&Value> {
        self.inner.get(k)
    }

    pub fn get_str(&self, k: &str) -> Option<&str> {
        self.inner
            .get(k)
            .map(|v| if let Value::String(n) = v { n } else { "" })
    }

    pub fn get_number(&self, k: &str) -> Option<i32> {
        self.inner
            .get(k)
            .map(|v| if let Value::Number(n) = v { *n } else { 0 })
    }

    pub fn get_float(&self, k: &str) -> Option<f32> {
        self.inner
            .get(k)
            .map(|v| if let Value::Float(n) = v { *n } else { 0f32 })
    }
}
