pub mod command;

pub use self::command::Command;
pub use self::command::CommandName;

#[derive(Debug)]
pub struct CommandProcessor {
    buf: String,
}

impl CommandProcessor {
    pub fn new() -> Self {
        Self { buf: String::new() }
    }

    pub fn process_raw_command(&mut self) -> Result<Command, String> {
        std::io::stdin().read_line(&mut self.buf).unwrap();
        let command = Command::from(&self.buf)?;
        self.buf.clear();
        return Ok(command);
    }
}
