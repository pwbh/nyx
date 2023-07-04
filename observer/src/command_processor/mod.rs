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
        std::io::stdin().read_line(&mut self.buf).unwrap();
        let command = Command::from(&self.buf)?;
        self.add_history(self.buf.clone());
        self.buf.clear();
        return Ok(command);
    }

    fn add_history(&mut self, raw_command: String) {
        self.history.push(raw_command);
    }
}
