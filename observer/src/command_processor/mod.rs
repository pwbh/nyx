pub mod command;

pub use self::command::Command;
pub use self::command::CommandName;

#[derive(Debug)]
pub struct CommandProcessor {
    buf: String,
    history: Vec<String>,
}

impl CommandProcessor {
    pub fn new() -> Self {
        Self {
            buf: String::new(),
            history: Vec::with_capacity(64),
        }
    }

    pub fn process_raw_command(&mut self) -> Result<Command, String> {
        std::io::stdin()
            .read_line(&mut self.buf)
            .map_err(|e| e.to_string())?;

        if self.buf.trim().len() == 0 {
            return Err("Empty command".to_string());
        }

        let command = Command::from(&self.buf)?;
        self.add_history((&self.buf).to_string());
        self.buf.clear();
        return Ok(command);
    }

    fn add_history(&mut self, raw_command: String) {
        self.history.push(raw_command);
    }
}
